# pairwise-roomlist-cursor-order

## 领域

游戏、图形与多媒体

## 主题

房间列表游标

## 说明

稳定排序键、广播幂等与重连全量同步。

初始状态下 `gameplay/roomlist_cursor_order.py` 中所有函数抛出 `NotImplementedError`，测试全部失败。
修复实现后运行：

```bash
python -m pytest -q
```

期望全部通过。
