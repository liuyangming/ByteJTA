
**ByteJTA** is an implementation of Distributed Transaction Manager, based on the XA/2PC mechanism. 

**ByteJTA** is comptible with JTA and could be seamlessly integrated with Spring and other Java containers.


## 1. Quick Start

#### 1.1 Add maven depenency
###### 1.1.1. Spring Cloud
```xml
<dependency>
	<groupId>org.bytesoft</groupId>
	<artifactId>bytejta-supports-springcloud</artifactId>
	<version>0.5.0-BETA5</version>
</dependency>
```
###### 1.1.2. dubbo
```xml
<dependency>
	<groupId>org.bytesoft</groupId>
	<artifactId>bytejta-supports-dubbo</artifactId>
	<version>0.5.0-BETA5</version>
</dependency>
```



## 2. Documentation & Samples
* [Document](https://github.com/liuyangming/ByteJTA/wiki)
* [Sample](https://github.com/liuyangming/ByteJTA-sample)



## 3. Features
* support declarative transaction management
* support distributed transaction scenarios. e.g. multi-datasource, cross-applications and cross-servers transaction
* support Dubbo framework
* support Spring Cloud



## 4. Contact Me
If you have any questions or comments regarding this project, please feel free to contact me at:

1. send mail to _[bytefox#126.com](bytefox@126.com)_
~OR~
2. add Tecent QQ group 537445956/606453172

We will review all the suggestions and implement good ones in future release.
