ByteJTA是一个基于XA/2PC机制的分布式事务管理器。实现了JTA接口，可以很好的与EJB、Spring等容器（本文档下文说明中将以Spring容器为例）进行集成。

## 一、文档 & 样例
* 使用文档： https://github.com/liuyangming/ByteJTA/wiki
* 使用样例： https://github.com/liuyangming/ByteJTA-sample

## 二、ByteJTA特性
* 1、支持Spring容器的声明式事务管理；
* 2、支持多数据源、跨应用、跨服务器等分布式事务场景；
* 3、支持spring cloud；
* 4、支持dubbo服务框架；

## 三、建议及改进
若您有任何建议，可以通过1）加入qq群537445956向群主提出，或2）发送邮件至bytefox@126.com向我反馈。本人承诺，任何建议都将会被认真考虑，优秀的建议将会被采用，但不保证一定会在当前版本中实现。
