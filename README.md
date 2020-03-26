# UserPortrait
### 基于Flink的用户画像系统

&emsp;&emsp;主要基于用户行为数据为用户每个打上年代标签、手机运营商标签、邮件运营商标签、用户败家指数标签和潮男潮女族标签等，并使用逻辑回归算法预测用户性别从而填补用户性别信息空缺值，使用K-Means算法实现用户分群等需求，最终将画像标签数据存入数据仓库。

&emsp;&emsp;如下是基于Flink的用户画像系统架构图：

![基于Flink的用户画像系统架构图](https://img-blog.csdnimg.cn/20200326104045943.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzQwNjQwMjI4,size_16,color_FFFFFF,t_70)

&emsp;&emsp;如下是基于Flink的用户画像数据来源图：

![基于Flink的用户画像数据来源图](https://img-blog.csdnimg.cn/20200326104312729.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzQwNjQwMjI4,size_16,color_FFFFFF,t_70)
