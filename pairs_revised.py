# -*- coding:utf-8 -*-
import sys
import time
import json
import traceback
from kungfu.wingchun.constants import *
from util.tool_datetime import ToolDateTime
from util.tool_redis_client import ToolRedisClient
from util.tool_logging import ToolLogging
import numpy as np
from datetime import datetime


class StockIndex(object):
    def __init__(self):
        self.account = '165595'
        self.source = Source.CTP
        self.exchange = Exchange.CFFEX

        self.ticker = ["IF2009", "IF2012"]
        self.maker = self.ticker[1]
        self.taker = self.ticker[0]
        self.subject = "{}_{}".format(self.ticker[0], self.ticker[1])

        self.tick_price = 0.2    
        self.context = None
        self.data = {}  # 行情缓存
        self.info = {}  # 实时记录ema, 持仓，价差
        self.snapshot = 500000000  # 500ms一次快照
        self.reset_ema = False     # 是否使用之前的EMA进行平滑
        self.volume_record = {self.maker: {'expect_volume': 0, 'trade_volume': 0, 'frozen': 0},
                              self.taker: {'expect_volume': 0, 'trade_volume': 0, 'frozen': 0}}  # 实时成交数量记录
        # 记录的配对订单, 检查订单状态
        self.pairs_orders = {self.maker: {}, self.taker: {}}
        
        self.cancelled_orders = {}  # 记录的撤单
        self.partial_orders = {}
        self.error_stat = {'count': 0}
        self.trade_records = {self.maker: [], self.taker: []}
        self.trade_finish_flag = False
        self.today_is_open = False  # 今天是否开过仓， 默认未开仓
        self.request_volume = 2

        # 模型参数
        self.ALPHA = 0.02
        self.min_profit = 0.2
        self.shift = 0.2  
        self.convex = 0.2 
        self.spread_pos = 0  # 相对仓位
        self.max_pos = 5   # 最大开仓配对数量
        self.freq = 60   # EMA得更新频率
        self.open_interval = 60 # 开仓间隔
        self.last_trade_ts = time.time() - self.open_interval
        
        self.STOP = False  # 全局判断, 是否停止开仓  False: 正常开仓， True： 停止开仓
        self.over_price = 2  # 超价打单

        self.trade_morning = [int(datetime.now().replace(hour=9, minute=30, second=0, microsecond=0).timestamp() * 1e9),
                              int(datetime.now().replace(hour=11, minute=30, second=0, microsecond=0).timestamp() * 1e9)]
        self.trade_noon = [int(datetime.now().replace(hour=13, minute=0, second=0, microsecond=0).timestamp() * 1e9),
                           int(datetime.now().replace(hour=15, minute=0, second=0, microsecond=0).timestamp() * 1e9)]
        # 订单状态
        self.wait_state = [OrderStatus.Unknown, OrderStatus.Submitted, OrderStatus.Pending]
        self.cancel_state = [OrderStatus.Cancelled, OrderStatus.Error]

        # log
        path = sys.path[2] + f'/log/{self.subject}/'
        self.logger = ToolLogging(file=path + 'ctp.log', level='info', classify='date', date_dir=True).get_logger()
        self.order_logger = ToolLogging(file=path + 'order.log', level='info', classify='date', date_dir=True).get_logger()
        self.trade_logger = ToolLogging(file=path + 'trade.log', level='info', classify='date', date_dir=True).get_logger()
        self.ema_logger = ToolLogging(file=path + 'ema.log', level='info', classify='date', date_dir=True).get_logger()
        # self.redis = ToolRedisClient(sys.path[2] + '/config/redis_quote.conf')
        
    def init_position(self):
        self.logger.info(f'################init_position#################')
        # self.info = self.redis.get(self.subject)
        with open("config/{}.json".format(self.subject), "r") as f:
            self.info = json.load(f)
        self.spread_pos = self.info.get('pos')

        self.logger.warning(f'init spread ema, (spread){self.info}')
        self.logger.warning(f'init position, (spread_pos){self.spread_pos}')

    def print_info(self, title):
        self.ema_logger.info(f"################{title}, ema:{self.info['ema']}, pos:{self.info.get('pos')}#################")
        self.ema_logger.info(f"{self.data}")
        self.ema_logger.info(f"adjsp: {self.info['bid_adj']}, {self.info['ask_adj']}")
        self.ema_logger.info(f"bound: {self.info['lower']}, {self.info['upper']}")
        self.ema_logger.info(f"{self.info['bid_adj'] < self.info['lower']}, {self.info['ask_adj'] > self.info['upper']}")

    def record_info(self, quote):
        try:
            data_time = quote.data_time 
            instrument_id = quote.instrument_id
            ask_price = quote.ask_price[0]
            ask_volume = quote.ask_volume[0]
            bid_price = quote.bid_price[0]
            bid_volume = quote.bid_volume[0]

            self.logger.info(f"time gap: {time.time()*10e8 - data_time}")

            # # 如果价格非法， 或者时间
            # if ask_price > 99999 or bid_price > 99999 or data_time < self.trade_morning[0] or data_time > self.trade_noon[1]:
            #     return
            # if self.trade_morning[1] <= data_time <= self.trade_noon[0]:
            #     self.data = {}
            #     return
            
            # 记录行情
            self.data[instrument_id] = {'ask_price': ask_price, 'ask_volume': ask_volume, 'bid_price': bid_price,
                                        'bid_volume': bid_volume, 'last_price': quote.last_price, 'data_time': data_time}

            if self.data.get(self.maker) and self.data.get(self.taker):
                self.info['pos'] = self.spread_pos    # 得到当前仓位
                # 计算ema 和边界条件
                self.cal_spread_info(data_time)
                # 判断开仓条件
                self.judge_cross()
                # self.redis.set(self.subject, self.info)
                with open(f"config/{self.subject}.json", "w") as f:
                    json.dump(self.info, f)
                self.logger.info(f'spread_info, {self.info}')
               
        except Exception as e:
            self.logger.error(f'record ma, {traceback.format_exc()}')

    def cal_spread_info(self, data_time):
       
        try:
            self.info['update_time'] = time.time()
            # 如果第一次开仓
            if not self.info.get('time') or self.reset_ema:
                self.info['time'] = data_time
                new_ema = (self.data.get(self.taker).get('ask_price') + self.data.get(self.taker).get('bid_price'))/2 - (self.data.get(self.maker).get('ask_price') + self.data.get(self.maker).get('bid_price'))/2

                self.info['new_ema'] = self.rr(new_ema)
                self.info['ema'] = self.rr(new_ema)
                self.info['lower'] = self.rr(self.info['ema'] - self.min_profit)
                self.info['upper'] = self.rr(self.info['ema'] + self.min_profit)
                self.reset_ema = False
            else:
                # 如果此Tick的时间 - 上次记录的时间 >= 60s, 计算一次ema
                if data_time - self.info.get('time') >= self.freq * self.snapshot * 2:
                    self.info['time'] = data_time
                    # 用最新的成交价计算ema
                    new_ema = (self.data.get(self.taker).get('ask_price') + self.data.get(self.taker).get('bid_price'))/2 - (self.data.get(self.maker).get('ask_price') + self.data.get(self.maker).get('bid_price'))/2
                    self.info['new_ema'] = self.rr(new_ema)
                    self.info['ema'] = self.rr(new_ema * self.ALPHA  + (1 - self.ALPHA) * self.info.get('ema'))
                    # 计算ema的上下边界
                    self.info['lower'] = self.rr(self.info['ema'] - self.min_profit)
                    self.info['upper'] = self.rr(self.info['ema'] + self.min_profit)
                    self.print_info('ema_update')
                # 计算4个实时 开仓值
                long_pos = self.spread_pos + 1
                short_pos = self.spread_pos - 1
                long_adjust = self.rr(long_pos * (self.shift + 0.5 * self.convex * long_pos * np.sign(long_pos)))
                short_adjust = self.rr(short_pos * (self.shift + 0.5 * self.convex * short_pos * np.sign(short_pos)))
                long_spread = self.data.get(self.taker).get('ask_price') - self.data.get(self.maker).get('bid_price')
                short_spread = self.data.get(self.taker).get('bid_price') - self.data.get(self.maker).get('ask_price')
                self.info['bid'] = long_spread
                self.info['ask'] = short_spread
                self.info['bid_adj'] = self.rr(long_spread + long_adjust)
                self.info['ask_adj'] = self.rr(short_spread + short_adjust)
               
        except Exception as e:
            self.logger.error(f'cal ma, {traceback.format_exc()}')

    def judge_cross(self):
        """
        判断挂单条件， 先判断hit， 再判断contrib
        :return:
        """
        try:
            if self.info.get('bid_adj') and self.info.get('bid_adj') < self.info['lower']:
                if self.data.get(self.taker).get('ask_volume', 0) >= self.request_volume:
                    self.place_maker_order('bid')
                    self.print_info('buy')
            elif self.info.get('ask_adj') and self.info.get('ask_adj') > self.info['upper']:
                if self.data.get(self.taker).get('bid_volume', 0) >= self.request_volume:
                    self.place_maker_order('ask')
                    self.print_info('sell')
            else:
                pass
        except Exception as e:
            self.logger.error(f'judge hit, {e}')

    def place_maker_order(self, direction):
        try:
            if self.STOP:
                return
            if time.time() - self.last_trade_ts <= self.open_interval:
                last_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.last_trade_time))
                self.logger.info(f'open too fast ! last open time:{last_time}')
                return
            action = self.open_or_close(direction)
            if not action:
                return
            self.logger.info(f'####place pairs orders, {direction} ##########')
            self.pairs_orders = {self.maker: {}, self.taker: {}}

            m_side = Side.Sell if direction == 'bid' else Side.Buy
            m_price = self.get_price(self.maker, m_side)
            m_order_id = self.place_order(self.maker, m_price, 1, m_side, action, PriceType.Fak)

            self.pairs_orders[self.maker][m_order_id] = {}
            self.volume_record[self.maker]['expect_volume'] = 1
            self.volume_record[self.maker]['frozen'] = 1
           
            self.logger.warning(f'insert maker order, {direction}, (pair orders) {self.pairs_orders}, (vol record){self.volume_record}')
        except Exception as e:
            self.logger.error(f'Place pairs orders, {direction}, {traceback.format_exc()}')

    def cancel_my_order(self, order_id):
        self.context.cancel_order(order_id)
        self.logger.info(f'timer cancel order, {order_id}')

    def place_order(self, instrument, price, volume, side, offset, order_type=PriceType.Limit):
        order_id = self.context.insert_order(instrument, self.exchange, self.account, price, volume, order_type, side, offset)
        self.logger.warning(f'insert order, (id){order_id}, (price){price}, (type){order_type}, (side){side}, (offset){offset}')
        return order_id

    def open_or_close(self, direction):
        instrument_num = self.info.get(self.maker, {}).get('buy', {}).get('volume', 0) + \
                             self.info.get(self.maker, {}).get('sell', {}).get('volume', 0) + \
                             self.info.get(self.taker, {}).get('sell', {}).get('volume', 0) + \
                             self.info.get(self.taker, {}).get('buy', {}).get('volume', 0)
        
        # 开过今仓 不能再平仓
        if self.today_is_open:   # 锁仓或加仓
            # 计算开仓之后，多空配对的数量之和，判断是否大于最大配对持仓量
            if self.max_pos >= instrument_num / 2 + 1:
                return Offset.Open
            return
        else:
            if direction == 'bid':
                # 若有锁仓则解锁
                if self.info.get(self.maker, {}).get('buy', {}).get('volume', 0) > 0 and self.info.get(self.taker, {}).get('sell', {}).get('volume', 0) > 0:
                    return Offset.Close
                if self.max_pos >= instrument_num / 2 + 1:
                    return Offset.Open
                return
            else:
                # 若有锁仓则解锁
                if self.info.get(self.maker, {}).get('buy', {}).get('volume', 0) > 0 and self.info.get(self.taker, {}).get('sell', {}).get('volume', 0) > 0:
                    return Offset.Close
                if self.max_pos >= instrument_num / 2 + 1:
                    return Offset.Open
                return

    def get_price(self, instrument, side):
        if side == Side.Buy:
            price = self.data.get(instrument).get('ask_price')
        else:
            price = self.data.get(instrument).get('bid_price')
    
        return price

    def order_transfer(self, order):
        try:
            new_order = {
                'order_id': order.order_id,
                'instrument_id': order.instrument_id,
                'status': order.status,
                'side': order.side,
                'offset': order.offset,
                'price_type': order.price_type,
                'limit_price': order.limit_price,
                'volume': order.volume,
                'volume_traded': order.volume_traded,
                'volume_left': order.volume_left,
                'insert_time': order.insert_time
            }
            return new_order
        except Exception as e:
            self.logger.error(f'order transfer,{order}, {e}')

    def check_error(self, org_order):
        try:
            count = self.error_stat['count']
            if org_order.status == OrderStatus.Error:
                # 如果平仓数量算的不对  停止下单
                if org_order.error_id == 30:
                    self.STOP = True
                    raise Exception('Invalid position')
                now = time.time()
                if count == 0:
                    self.error_stat['first_time'] = now
                self.error_stat['count'] += 1
                if self.error_stat['count'] >= 3 and now - self.error_stat['first_time'] < 10:
                    self.STOP = True
                    raise Exception('Continuously order Error')
            else:
                if count == 0:
                    return
                if time.time() - self.error_stat['first_time'] > 10:
                    self.error_stat['count'] = 0
        except Exception as e:
            self.logger.error(f'check error failed, {org_order}, {traceback.format_exc()}')

    def update_orders(self, org_order):
        try:
            # 检查order error id
            self.check_error(org_order)
            order = self.order_transfer(org_order)
            order_id = order.get('order_id')
            instrument_id = order.get('instrument_id')
            if order_id in self.pairs_orders.get(instrument_id, {}):
                if self.pairs_orders[instrument_id][order_id] == order:
                    pass
                else:
                    self.pairs_orders[instrument_id][order_id] = order
                    self.check_pairs_orders(instrument_id, order)
                    if order.get('order_status') in [OrderStatus.Cancelled, OrderStatus.Error, OrderStatus.Filled,
                                                     OrderStatus.PartialFilledNotActive]:
                        self.pairs_orders[instrument_id].pop(order_id)
            else:
                self.logger.info(f'Update orders error, {order}, {self.pairs_orders}')
            if order_id in self.cancelled_orders:
                if order.get('status') in self.cancel_state:
                    self.cancelled_orders.pop(order_id)
        except Exception as e:
            self.logger.error(f'update orders, {org_order}, {order}, {traceback.format_exc()}')

    def clear_volume_record(self):
        self.volume_record = {self.maker: {'expect_volume': 0, 'trade_volume': 0, 'frozen': 0},
                              self.taker: {'expect_volume': 0, 'trade_volume': 0, 'frozen': 0}}

    def check_hit(self, instrument, order):
        status = order.get('status')
        if status in self.wait_state:
            # 看是否要超时撤单
            pass
        elif status in [OrderStatus.Cancelled, OrderStatus.PartialFilledNotActive]:
            # 加价打单  检查cancel单
            volume_left = order.get('volume_left')
            self.volume_record[instrument]['frozen'] -= order.get('volume')
            self.volume_record[instrument]['trade_volume'] += order.get('volume_traded')
            if order.get('volume_left') > 0:
                tmp_price = order.get('limit_price')
                if order.get('side') == Side.Buy:
                    quote_price = self.data.get(instrument).get('ask_price')
                    price = max(self.rr(tmp_price + self.over_price * self.tick_price), quote_price)
                else:
                    quote_price = self.data.get(instrument).get('bid_price')
                    price = min(self.rr(tmp_price - 2 * self.tick_price), quote_price)
                order_id = self.place_order(instrument, price, volume_left, order.get('side'), order.get('offset'))
                self.instruments_orders[instrument][order_id] = {}
                self.volume_record[instrument]['frozen'] += volume_left
                self.context.add_timer(self.context.now() + int(7.5 * 1e7), lambda ctx, event: self.cancel_my_order(order_id))
            self.logger.info(f"{order.get('order_id')}, {status}, {self.volume_record}")
        elif status in [OrderStatus.Pending, OrderStatus.PartialFilledActive]:
            # 超时撤单
            if time.time()*1e9 - order.get('insert_time') > int(1e8):
                order_id = order.get('order_id')
                action_id = self.context.cancel_order(order_id)
                self.logger.info(f'overtime cancel order, (order id){order_id}, (action id){action_id}')
        elif status == OrderStatus.Filled:
            # 检查 expect volume == trade volume ?
            match_instrument = self.taker
            self.volume_record[instrument]['frozen'] -= order.get('volume')
            self.volume_record[instrument]['trade_volume'] += order.get('volume_traded')
            self.logger.info(f'{status}, {self.volume_record}')
            if self.volume_record[instrument]['expect_volume'] == self.volume_record[instrument]['trade_volume']:
                # 配对订单完成
                if self.volume_record[match_instrument]['expect_volume'] == self.volume_record[match_instrument]['trade_volume']:
                    if self.volume_record[self.maker]['trade_volume'] == self.volume_record[self.taker]['trade_volume']:
                        self.logger.warning(f'{instrument} filled, {match_instrument} filled')
                        volume = self.volume_record[self.maker]['trade_volume']
                        if order.get('side') == Side.Sell:
                            self.spread_pos -= volume
                        else:
                            self.spread_pos += volume
                        self.trade_finish_flag = True
                        self.last_trade_time = time.time()
                        self.clear_volume_record()
                        if order.get('offset') == Offset.Open:
                            self.today_is_open = True
                    else:
                        self.logger.info(f'Error warning paritial filled, (instrument){instrument}, (order){order}, (volume record'
                                         f'){self.volume_record}')
                else:
                    self.logger.info(f'{instrument} filled, waiting {match_instrument}, (volume record){self.volume_record}')
            else:
                self.logger.info(f'partial filled, (instrument){instrument}, (order){order}, (volume record){self.volume_record}')
        elif status == OrderStatus.Error:
            self.logger.warning(f"Error order, {order.get('order_id')}")
        else:
            self.logger.error(f'Invalid order state, {order}')

    def place_taker_order(self, t_volume, order):
        self.volume_record[self.taker]['expect_volume'] += t_volume
        side = Side.Buy if order.get('side') == Side.Sell else Side.Sell
        price = self.get_price(self.taker, side)
        if side == Side.Buy:
            price = self.rr(price + self.over_price * self.tick_price)
        else:
            price = self.rr(price - self.over_price * self.tick_price)
        order_id = self.place_order(self.taker, price, t_volume, side, order.get('offset'))
        self.instruments_orders[self.taker][order_id] = {}
        self.volume_record[self.taker]['frozen'] += t_volume
        self.context.add_timer(self.context.now() + int(7.5 * 1e7), lambda ctx, event: self.cancel_my_order(order_id))

    def check_pairs_orders(self, instrument, order):
    
        if instrument == self.maker:
            status = order.get('status')
            if status in [OrderStatus.Cancelled, OrderStatus.PartialFilledNotActive, OrderStatus.PartialFilledActive, OrderStatus.Filled]:
                self.logger.info(f'(instrument) {instrument}, (order){order}, (volume){self.volume_record}')
            
            if status == OrderStatus.Cancelled:
                # 检查成交了多少
                volume = order.get('volume')
                volume_left = order.get('volume_left')
                volume_trade = order.get('volume_traded')
                if volume == volume_left:
                    # cancelled 都未成交
                    self.logger.info(f'{status}, {self.volume_record}')
                    self.clear_volume_record()
                    return
            elif status == OrderStatus.Filled:
                self.place_taker_order(1, order)
                self.logger.info(f'{status}, {self.volume_record}, (data){self.data}')
                # 下单完成， 检查配对
                if self.volume_record[self.taker]['expect_volume'] > 0 and self.volume_record[self.taker]['expect_volume'] == self.volume_record[self.taker]['trade_volume']:
                    if self.volume_record[instrument]['trade_volume'] == self.volume_record[self.taker]['trade_volume']:
                        self.logger.warning(f'second filled, first filled')
                        volume = self.volume_record[instrument]['trade_volume']
                        if order.get('side') == Side.Sell:
                            self.spread_pos += volume
                        else:
                            self.spread_pos -= volume
                        self.trade_finish_flag = True
                        self.last_trade_time = time.time()
                        self.clear_volume_record()
                        if order.get('offset') == Offset.Open:
                            self.today_is_open = True
                else:
                    self.logger.info(f'first filled, waiting second')
            elif status in self.wait_state:
                pass
            elif status == OrderStatus.Error:
                self.logger.warning(f"Error order, {order.get('order_id')}")
            else:
                self.logger.error(f'Invalid order state, {order}')
        else:
            # taker instrument
            self.check_hit(instrument, order)

    def my_cancel_order(self, order_id):
        if order_id in self.cancelled_orders and (time.time() - self.cancelled_orders.get(order_id).get('time')) < 1:
            self.logger.info(f"already cancelled")
        else:
            action_id = self.context.cancel_order(order_id)
            self.cancelled_orders[order_id] = {'cancel_id': action_id, 'time': time.time()}
            self.logger.info(f"[cancel order] (action_id){action_id} (order_id){order_id}")

    def trade_transfer(self, trade):
        try:
            offset_transfer = {Offset.Open: 'open', Offset.Close: 'close'}
            side_transfer = {Side.Buy: 'buy', Side.Sell: 'sell'}
            new_trade = {
                'order_id': trade.order_id,
                'instrument_id': trade.instrument_id,
                'side': side_transfer.get(trade.side),
                'offset': offset_transfer.get(trade.offset),
                'price': trade.price,
                'volume': trade.volume,
            }
            return new_trade
        except Exception as e:
            self.logger.error(f'order transfer,{trade}, {e}')

    def append_instruments(self, trade):
        trade = self.trade_transfer(trade)
        instrument = trade.get("instrument_id")
        side = trade.get("side")
        price = float(trade.get("price"))
        volume = int(trade.get("volume"))
        offset = trade.get("offset")
        if self.info.get(instrument, {}).get(side):
            if offset == 'open':
                self.info[instrument][side]['price'] = self.rr((self.info[instrument][side]['price'] * self.info[instrument][side]['volume'] + price * volume)
                                                        / (volume + self.info[instrument][side]['volume']), 2)
                self.info[instrument][side]['volume'] += volume
            else:
                match_side = 'sell' if side == 'buy' else 'buy'
                self.info[instrument][match_side]['volume'] -= volume
        else:
            self.info.setdefault(instrument, {}).setdefault(side, {'price': price, 'volume': volume})
        if self.trade_finish_flag:
            self.trade_finish_flag = False
    
    @staticmethod
    def rr(x):
        return round(x, 1)


spread_arb = StockIndex()


def pre_start(context):
    context.log.info('pre start')
    spread_arb.context = context
    context.add_account(spread_arb.source, spread_arb.account, 100000.0)    # 添加交易柜台， 添加账户
    context.subscribe(spread_arb.source, spread_arb.ticker, spread_arb.exchange)  # 订阅行情


def post_start(context):
    context.log.info('post start')
    spread_arb.init_position()


def on_quote(context, quote):
    spread_arb.record_info(quote)


def on_order(context, order):
    spread_arb.order_logger.info(f'on order, (symbol){order.instrument_id}, (id){order.order_id} (status){order.status}, (type){order.price_type}, {order}')
    spread_arb.update_orders(order)


def on_trade(context, trade):
    # 用trade 去算收益
    spread_arb.trade_logger.info(f'on trade, (instrument_type){trade.instrument_type}, (side){trade.side}, (offset){trade.offset}, {trade}')
    spread_arb.append_instruments(trade)


# 应该未调用
def on_transaction(context, transaction):
    context.log.error("[on_transaction] {} {}".format(transaction.instrument_id, transaction.exchange_id))


# 应该未调用
def on_entrust(context, entrust):
    context.log.error("[on_entrust] {} {}".format(entrust.instrument_id, entrust.exchange_id))


# 策略释放资源前回调，仍然可以获取持仓和报单
def pre_stop(context):
    context.log.info("[befor strategy stop]")


# 策略释放资源后回调
def post_stop(context):
    context.log.info("[befor process stop]")
