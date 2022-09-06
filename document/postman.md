### 进行首个API调用

- 在本指南中，我们将逐步引导您进行首次 API 调用。希望能让您对API有更具体的认识，有助于您后续充分利用API。

1. 确认调用接口

    - 先简单介绍API调用接口，调用接口代表了您希望从API中获取的服务，一般接口格式如下：

      http://175.25.50.117:14127/write


2. 了解请求格式

    - 明确了API请求接口后，下面就是要明确接口的请求信息了，我们先来简单介绍下API的请求格式：
    - 发送的请求是一个JSON格式的字符串，其中header 和 body属性都是必需的。

      ```json
      {
          "header": {
              "username": "******",
              "password": "******"
          },
          "body": {
              "OpTargetQuote": [...],
              "OpTargetyDerivativeIndicators_5min": [...]
          }
      }
      ```

    - header（账户信息）中需要填写您的**API账户名**、**密码**等信息。

    - body（具体请求参数）的内容取决于接口要求。

3. 设置header信息

    - header信息是比较简单的，针对具体API权限的账户请求的header信息是固定的，不会受接口变化的影响，header信息如下。

      ```json
      {
          "header": {
              "username":"******",                     
              "password":"******"                    
          }
      }
      ```

4. 设置body信息

    - 输入信息：getFactorInfoRequest

      | 表名  |   类型   |                            说明                            |                     
      |:------:|:--------------------------------:| :----------------: | 
      | OpTargetQuote | string | [datetime:Datetime,targetcode:str,price:float,pct:float] |

    - 同时可以参考代码示例，确定获取通过获取因子名称和代码的请求信息应为：

      ```json
      {    
          "body": {
             "OpTargetQuote": ["2022-09-06 17:23","SHFE.rb2012",12.5,36.7]
          }
      }
      ```

5. 调用请求
    - 一个完整的请求类似
   ```json
      {
          "header": {
              "username": "ybc",
              "password": "123456"
          },
          "body": {
              "OpTargetQuote": ["2022-09-06 17:23","SHFE.rb2012",12.5,36.7]
          }
      }
      ```
    - 以上我们确定了具体的接口和请求信息，下面我们就可以正式尝试进行调用了