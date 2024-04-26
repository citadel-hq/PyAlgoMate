import logging
import datetime
import traceback
from typing import Set, ForwardRef

import pyalgotrade.bar
from pyalgotrade.bar import BasicBar, Bars
from pyalgotrade.strategy.position import Position
from pyalgotrade.broker import Order
from pyalgomate.strategies.BaseOptionsGreeksStrategy import BaseOptionsGreeksStrategy
from pyalgomate.core import State
from pyalgomate.core import resampled

from pyalgomate.cli import CliMain
import pyalgomate.utils as utils


def getRoundedDateTime(originalDateTime: datetime.datetime, interval: datetime.timedelta):
    timeIntervalMinutes = int(interval.total_seconds() / 60)
    elapsedMinutes = originalDateTime.minute % timeIntervalMinutes
    return originalDateTime - datetime.timedelta(minutes=elapsedMinutes, seconds=originalDateTime.second,
                                                 microseconds=originalDateTime.microsecond)


class StraddlePosition:
    def __init__(self, ceHedge, peHedge, ceShort, peShort, strike):
        self.ceHedge = ceHedge
        self.peHedge = peHedge
        self.ceShort = ceShort
        self.peShort = peShort
        self.strike = strike


class StraddleStrategy(BaseOptionsGreeksStrategy):
    def __init__(self, feed, broker,
                 underlying,
                 lots=1,
                 strategyName=None,
                 telegramBot=None,
                 callback=None,
                 telegramChannelId=None,
                 telegramMessageThreadId=None):
        super(StraddleStrategy, self).__init__(feed, broker,
                                               strategyName=strategyName if strategyName else __class__.__name__,
                                               logger=logging.getLogger(__name__),
                                               callback=callback,
                                               telegramBot=telegramBot,
                                               telegramChannelId=telegramChannelId,
                                               telegramMessageThreadId=telegramMessageThreadId
                                               )
        self.exitTime = datetime.time(hour=15, minute=24)
        self.underlying = underlying
        self.rollingStraddleSymbol = f'{self.underlying} RS'

        underlyingDetails = self.getBroker().getUnderlyingDetails(self.underlying)
        self.underlyingIndex = underlyingDetails['index']
        self.strikeDifference = underlyingDetails['strikeDifference']
        self.lotSize = underlyingDetails['lotSize']

        self.lots = lots
        self.quantity = self.lotSize * self.lots
        self.stopLimitBufferPercentage = 15
        self.marketProtectionPercentage = 15
        self.tickSize = 0.05

        self.maxEntries = 3
        self.hedgesNStrikesAway = 8

        self.stopLosses = {
            datetime.time(hour=9, minute=15): 40,
            datetime.time(hour=9, minute=30): 40,
            datetime.time(hour=9, minute=45): 40,
            datetime.time(hour=10, minute=0): 40,
            datetime.time(hour=10, minute=15): 40,
            datetime.time(hour=10, minute=30): 45,
            datetime.time(hour=10, minute=45): 50,
            datetime.time(hour=11, minute=0): 50,
            datetime.time(hour=11, minute=15): 50,
            datetime.time(hour=11, minute=30): 50,
            datetime.time(hour=11, minute=45): 50,
            datetime.time(hour=12, minute=0): 50,
            datetime.time(hour=12, minute=15): 50,
            datetime.time(hour=12, minute=30): 50,
            datetime.time(hour=12, minute=45): 50,
            datetime.time(hour=13, minute=0): 60,
            datetime.time(hour=13, minute=15): 70,
            datetime.time(hour=13, minute=30): 80,
            datetime.time(hour=13, minute=45): 90,
            datetime.time(hour=14, minute=0): 100,
            datetime.time(hour=14, minute=15): 100,
            datetime.time(hour=14, minute=30): 100,
            datetime.time(hour=14, minute=45): 100
        }

        self.resampledBars = resampled.ResampledBars(
            self.getFeed(), pyalgotrade.bar.Frequency.MINUTE, self.onResampledBars)

        self.__reset__()

        self.log(f'🔔 {self.__class__.__name__} initialized successfully!\n'
                 f'Underlying: {self.underlying}\n'
                 f'Underlying index: {self.underlyingIndex}\n'
                 f'Strike Difference: {self.strikeDifference}\n'
                 f'Lot size: {self.lotSize}\n'
                 f'Quantity: {self.quantity}\n')

    def __reset__(self):
        super().reset()
        self.pendingEntry = set()
        self.pendingSLToCost = set()
        self.pendingCancelExit = set()
        self.pendingExit = set()
        self.lastSentTime = self.getFeed().getCurrentDateTime().time()

        self.positions: Set[StraddlePosition] = set()

    def onBars(self, bars: Bars):
        if self.isBacktest():
            return self.strategyLogic(bars)

    def onIdle(self):
        if not self.isBacktest():
            return self.strategyLogic()

    def strategyLogic(self, bars: Bars = None):
        try:
            if self.marketStartTime < self.getFeed().getCurrentDateTime().time() < self.marketEndTime:
                if not self.lastSentTime:
                    self.lastSentTime = datetime.datetime.now().time()

                lastSentTime = datetime.datetime.combine(
                    self.getFeed().getCurrentDateTime().date(), self.lastSentTime)
                lastSentRoundedTime = getRoundedDateTime(
                    lastSentTime, datetime.timedelta(minutes=15))
                timeToCheck = (
                        lastSentRoundedTime + datetime.timedelta(minutes=15)).time()

                if self.getFeed().getCurrentDateTime().time() >= timeToCheck:
                    self.lastSentTime = self.getFeed().getCurrentDateTime().time()
                    self.sendInfo()

            if self.state in [State.PLACING_ORDERS, State.SQUARING_OFF]:
                self.handlePlacingOrders()

            if self.getFeed().getCurrentDateTime().time() >= self.marketEndTime:
                if (len(self.getActivePositions()) + len(self.getClosedPositions())) > 0:
                    self.log(
                        f"🔔 Overall PnL\n\nOverall PnL for {self.getFeed().getCurrentDateTime().date()} is {self.getOverallPnL()}")
                    self.sendPnLImage()
                    self.__reset__()
            elif self.getFeed().getCurrentDateTime().time() >= self.exitTime:
                if self.state == State.ENTERED:
                    self.log(
                        f"🔔 Exit Time Reached\n\nCurrent time {self.getFeed().getCurrentDateTime().time()} is >= Exit time {self.exitTime}. "
                        "Closing all positions!")
                    self.closeAllPositions()
        except Exception as e:
            self.logger.error(e)
            self.logger.exception(traceback.format_exc())
        finally:
            if bars is None:
                return
            self.resampledBars.addBars(bars.getDateTime(), bars)

    def onResampledBars(self, bars: Bars):
        try:
            # Trading Logic
            triggered = True

            if len(self.positions) == self.maxEntries:
                return

            if triggered:
                self.positions.add(self.enterPositions())
        except Exception as e:
            self.logger.error(e)
            self.logger.exception(traceback.format_exc())

    def enterPositions(self):
        atmStrike = self.getATMStrike(self.getLastPrice(self.underlying), self.strikeDifference)

        if atmStrike is None:
            return None

        currentDate = self.getFeed().getCurrentDateTime().date()
        currentExpiry = utils.getNearestWeeklyExpiryDate(currentDate, self.underlyingIndex)

        ceHedgeSymbol = self.getBroker().getOptionSymbol(self.underlying, currentExpiry,
                                                         atmStrike + (self.hedgesNStrikesAway * self.strikeDifference),
                                                         'c')
        ceShortSymbol = self.getBroker().getOptionSymbol(self.underlying, currentExpiry, atmStrike, 'c')
        peHedgeSymbol = self.getBroker().getOptionSymbol(self.underlying, currentExpiry,
                                                         atmStrike - (self.hedgesNStrikesAway * self.strikeDifference),
                                                         'p')
        peShortSymbol = self.getBroker().getOptionSymbol(self.underlying, currentExpiry, atmStrike, 'p')

        self.state = State.PLACING_ORDERS
        ceHedgePosition = self.enterWithMarketProtection(ceHedgeSymbol, Order.Action.BUY)
        peHedgePosition = self.enterWithMarketProtection(peHedgeSymbol, Order.Action.BUY)
        ceShortPosition = self.enterWithMarketProtection(ceShortSymbol, Order.Action.SELL)
        peShortPosition = self.enterWithMarketProtection(peShortSymbol, Order.Action.SELL)

        self.pendingEntry.add(ceHedgePosition)
        self.pendingEntry.add(peHedgePosition)
        self.pendingEntry.add(ceShortPosition)
        self.pendingEntry.add(peShortPosition)
        return StraddlePosition(ceHedgePosition, peHedgePosition, ceShortPosition, peShortPosition, atmStrike)

    def getRoundedOffPriceByTickSize(self, price):
        return price - (price % self.tickSize)

    def exitWithMarketProtection(self, position: Position):
        lastBar = self.getFeed().getLastBar(position.getInstrument())
        if lastBar is None:
            self.logger.info(f'LTP of <{position.getInstrument()}> is None while exiting with market position.')
            return

        if position.getEntryOrder().isBuy():
            limitPrice = self.getRoundedOffPriceByTickSize(
                lastBar.getClose() * (1 - (self.marketProtectionPercentage / 100.0)))
        else:
            limitPrice = self.getRoundedOffPriceByTickSize(
                lastBar.getClose() * (1 + (self.marketProtectionPercentage / 100.0)))

        position.exitLimit(limitPrice)

    def enterWithMarketProtection(self, symbol, action: Order.Action):
        lastBar = self.getFeed().getLastBar(symbol)
        if lastBar is None:
            self.logger.info(f'LTP of <{symbol}> is None while entering with market position.')
            return

        if action == Order.Action.BUY:
            limitPrice = self.getRoundedOffPriceByTickSize(
                lastBar.getClose() * (1 + (self.marketProtectionPercentage / 100.0)))
            return self.enterLongLimit(symbol, limitPrice, self.quantity)
        else:
            limitPrice = self.getRoundedOffPriceByTickSize(
                lastBar.getClose() * (1 - (self.marketProtectionPercentage / 100.0)))
            return self.enterShortLimit(symbol, limitPrice, self.quantity)

    def closeAllPositions(self):
        self.state = State.SQUARING_OFF
        for position in list(self.getActivePositions()):
            if position.exitActive():
                self.pendingCancelExit.add(position)
                self.pendingExit.add(position)
                position.cancelExit()
            else:
                self.pendingExit.add(position)
                self.exitWithMarketProtection(position)

    def getStraddlePosition(self, position: Position):
        straddlePositions = [straddlePosition for straddlePosition in self.positions if
                             position in [straddlePosition.ceShort, straddlePosition.peShort]]
        return straddlePositions[0] if len(straddlePositions) else None

    def onEnterOk(self, position: Position):
        super().onEnterOk(position)
        self.pendingEntry.discard(position)

        straddlePosition: StraddlePosition = self.getStraddlePosition(position)

        if straddlePosition is None:
            return

        stopLossesList = [value for time, value in self.stopLosses.items() if time < self.getCurrentDateTime().time()]
        stopLossPercentage = stopLossesList[-1] if stopLossesList else 20

        entryPrice = position.getEntryOrder().getExecutionInfo().getPrice()
        stopLoss = entryPrice * (1 + (stopLossPercentage / 100.0))
        stopLoss = stopLoss - (stopLoss % 0.05)
        stopLossBuffer = stopLoss * \
                         ((100 + self.stopLimitBufferPercentage) / 100.0)
        stopLossBuffer = stopLossBuffer - (stopLossBuffer % 0.05)
        position.exitStopLimit(stopLoss, stopLossBuffer)

    def onEnterCanceled(self, position: Position):
        super().onEnterCanceled(position)
        self.pendingEntry.discard(position)

    def onExitCanceled(self, position: Position):
        super().onExitCanceled(position)
        self.pendingCancelExit.discard(position)

        if position in self.pendingSLToCost:
            entryPrice = position.getEntryOrder().getExecutionInfo().getPrice()
            entryPrice = entryPrice - (entryPrice % 0.05)
            stopLossBuffer = entryPrice * ((100 + self.stopLimitBufferPercentage) / 100.0)
            stopLossBuffer = stopLossBuffer - (stopLossBuffer % 0.05)

            self.state = State.PLACING_ORDERS if self.state != State.SQUARING_OFF else self.state
            position.exitStopLimit(entryPrice, stopLossBuffer)
            self.pendingSLToCost.discard(position)
        elif position in self.pendingExit:
            self.state = State.PLACING_ORDERS if self.state != State.SQUARING_OFF else self.state
            self.exitWithMarketProtection(position)

    def onExitOk(self, position: Position):
        super().onExitOk(position)
        self.pendingExit.discard(position)
        if self.state == State.SQUARING_OFF:
            return

        straddlePosition: StraddlePosition = self.getStraddlePosition(position)

        if straddlePosition is None:
            return

        otherPosition: Position = straddlePosition.ceShort if position != straddlePosition.ceShort \
            else straddlePosition.peShort

        if otherPosition.exitFilled():
            return

        if otherPosition.exitActive():
            self.state = State.PLACING_ORDERS
            self.pendingSLToCost.add(otherPosition)
            otherPosition.cancelExit()

    def handlePlacingOrders(self):
        if len(self.pendingEntry) or len(self.pendingSLToCost) or len(self.pendingCancelExit) or len(self.pendingExit):
            return

        if not self.isPendingOrdersCompleted():
            return

        if (self.state == State.SQUARING_OFF) and (self.getFeed().getCurrentDateTime().time() >= self.exitTime):
            self.state = State.EXITED
        elif len(list(self.getActivePositions())) == 0:
            self.state = State.LIVE
        else:
            self.state = State.ENTERED

    def sendInfo(self):
        self.sendPnLImage()


if __name__ == "__main__":
    CliMain(StraddleStrategy)
