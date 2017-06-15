ByteJTA是一个基于XA/2PC机制的分布式事务管理器。实现了JTA接口，可以很好的与EJB、Spring等容器（本文档下文说明中将以Spring容器为例）进行集成。

## 一、快速入门
#### 1.1. 加入maven依赖
```xml
<dependency>
	<groupId>org.bytesoft</groupId>
	<artifactId>bytejta-supports-dubbo</artifactId>
	<version>0.4.0-alpha2</version>
</dependency>
```
#### 1.2. 编写业务服务
```java
@Service("accountService")
public class AccountServiceImpl implements IAccountService {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Transactional
	public void increaseAmount(String accountId, double amount) throws ServiceException {
	    this.jdbcTemplate.update("update tb_account set amount = amount + ? where acct_id = ?", amount, acctId);
	}

}
```

## 二、文档 & 样例
* 使用文档： https://github.com/liuyangming/ByteJTA/wiki
* 使用样例： https://github.com/liuyangming/ByteJTA-sample


## 三、ByteJTA特性
* 1、支持Spring容器的声明式事务管理；
* 2、支持多数据源、跨应用、跨服务器等分布式事务场景；
* 3、支持长事务；
* 4、支持dubbo服务框架；

## 四、建议及改进
若您有任何建议，可以通过1）加入qq群537445956向群主提出，或2）发送邮件至bytefox@126.com向我反馈。本人承诺，任何建议都将会被认真考虑，优秀的建议将会被采用，但不保证一定会在当前版本中实现。
