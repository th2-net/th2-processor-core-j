# Description of th2-processor-core-j (0.3.0)

This is a common processor library which takes care of some features like requesting messages/events from a lw-data-provider (LwDP), subscribing to message queues, verify incoming streams vs response from LwDP, loading processor settings, etc.

# What is a processor?

The processor in th2 is a component that is responsible for handle th2 raw/parsed messages and events. Result of processing can be presented in any formats, for example, th2 events with analyze details, put data into external db, publish results via any external API.
Each processor can work in two modes:
* crawler - in this mode processor requests data from cradle via LwDP and receives it via message queue created dynamically.
* realtime - in this mode processor subscribes message queue pins and process data in passive mode.

Also, processor core provides optional ability to store and restore state. User can control the ability via `enableStoreState` and `stateSessionAlias` options. How to it works:
* state storing: processor core gets state from an implementation in serialized format, splits it into on or several raw messages and send them via `state` pin. This operation is executed at the end of processing each interval in crawler mode, at the shutdown component in realtime mode.
* state restoring: processor core requests last state from LwDP and passes to implementation factory at the application start.

# Configuration

The configuration can be split to four parts:
* common contains state settings and includes other parts
* crawler consists of parameters for requesting data and interval settings
* realtime contains flags to control subscriptions.
* processor settings depends on implementation

## Processor core settings

Processor core settings can be specified in `custom-config` section. 

**stateSessionAlias: _my-processor-state_** - the processor uses this session alias to store and restore core and implementation parts of state **Required parameter when enableStoreState is true**

**enableStoreState: _false_** - the processor continues work after restart from the interval where it stopped before if this option is true otherwise the processor start from time specified by the **from** option.
The default value is **false**

**useTransport: _false_** - option to switch between protobuf and transport message formats transferred via MQ 

For example:
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: my-processor
spec:
  custom-config:
    stateSessionAlias: my-processor-state
    enableStoreState: false
    useTransport: false
```

## Processor crawler mode settings

**from: _2021-06-16T12:00:00.00Z_** - the lower boundary for processing interval of time.
The processor works on the data starting from this point in time. **Required parameter**

**to: _2021-06-17T14:00:00.00Z_** - the higher boundary for processing interval of time.
The processor does not work on the data after this point in time. **If it is not set the processor will work until it is stopped.**

**intervalLength: _PT10M_** - the step that the processor will use to create intervals.
It uses the Java Duration format. You can read more about it [here](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-).
The default value is **PT10M**.

**syncInterval: _PT10M_** - the processor uses this value for a request to lw-data-provider. lw-data-provider splits the whole requested interval to sub-intervals with specified length and publishes data for the whole requested books and scores / session aliases / group for each sync-interval.
It uses the Java Duration format. You can read more about it [here](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-).
The default value is **PT10M**, also, the value should be less or equal as **intervalLength**.

**awaitTimeout: _10_** - the value of time wait interval for responds getting via MQ. The processor waits this time after lw-data-provider respond via gRPC until getting all requested data via MQ.
The default value is **10**.

**awaitUnit: _SECONDS_** - the time unit for **awaitTimeout** parameter.
Allowed values are described [here](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/temporal/ChronoUnit.html) in **Enum Constants** block.
The default value is **SECONDS**.

**intervalPrecessingDelay: _PT10M_** - a minimal difference between current time and the interval end to start processing that interval.
If interval end is after the current time or before current time but less than **intervalPrecessingDelay**
Processor will wait until the following condition is met: `(interval end + intervalPrecessingDelay) <= current time`.
Defaults to the value for **intervalLength** parameter.

For example:
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: my-processor
spec:
  custom-config:
    crawler:
        from: 2021-06-16T12:00:00.00Z
        to: 2021-06-17T14:00:00.00Z
          
        intervalLength: PT10M
        syncInterval: PT10M
        awaitTimeout: 10
        awaitUnit: SECONDS
```

Processor can work with data: events, messages, raw message at the same time. The processor requests configured data streams parallel for each interval. Users can be configured parameters for required data streams via special settings

### Crawler message settings

**messages** - processor will work on messages when this option is filled

**messageKinds: _[MESSAGE]_** - the types of messages will be requested from lw-data-provider. The option supports the next values: MESSAGE, RAW_MESSAGE
The default value is **[MESSAGE]**.

**bookToGroups** - the combination of each **book** and **group** will be requested from lw-data-provider. This map must not be empty, also, each **group** collection can be empty or filled via not blank values. **Required parameter** 

For example:

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: my-processor
spec:
  custom-config:
    crawler:
        messages:
          messageKinds: 
            - MESSAGE
          bookToGroups:
            book1:
              - group1
              - group2
            book2:
              - group1
              - group2
```

### Crawler events settings

**events** - processor will work on events when this option is filled

**bookToScopes** - the combination of each **book** and **scopes** will be requested from lw-data-provider. This map must not be empty, also, each **scope** collection can be empty or filled via not blank values. **Required parameter**

For example:

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: my-processor
spec:
  custom-config:
    crawler:
        events:
          bookToScope:
            book3:
              - scope1
              - scope2
            book4:
              - scope1
              - scope2
```

## Processor realtime mode settings

**enableMessageSubscribtion: _true_** - the processor subscribes to messages via pin marked by the `in` and `group` attribute.
The default value is **true**

**enableEventSubscribtion: _true_** - the processor subscribes to events via pin marked by the `in` and `event` attribute.
The default value is **false**

For example:
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: my-processor
spec:
  custom-config:
    realtime:
      enableMessageSubscribtion: true
      enableEventSubscribtion: false
```

## Processor implementation settings

Processor implementation settings can be specified in `processorSettings` field of `custom-config`. These settings will be loaded as an instance configured in the `IProcessorFactory.registerModules` during start up and then passed to every invocation
of `IProcessorFactory.create` method

Example of registration:

```kotlin
@AutoService(IProcessorFactory::class)
class MyProcessorFactory : IProcessorFactory {
   override fun registerModules(configureMapper: ObjectMapper) {
      configureMapper.registerModule(
         SimpleModule().addAbstractTypeMapping(
            IProcessorSettings::class.java,
            MyPocessorSetting::class.java
         )
      )
   }
}
```

Example of settings:

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: my-processor
spec:
  custom-config:
    processorSettings:
      cacheCapacity: 100
      strategy: "single"
```

## Required pins

Pins are a part of the main th2 concept. They describe what are the inputs and outputs of a box.
You can read more about them [here](https://github.com/th2-net/th2-documentation/wiki/infra:-Theory-of-Pins-and-Links#pins).

Processor core part uses one grpc-client pin to connect to the lw-data-provider

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: my-processor
spec:
  pins:
    grpc:
      client:
      - name: to_data_provider
        service-class: com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
        linkTo:
        - box: lw-data-provider
          pin: server
      - name: to_data_provider_stream
        service-class: com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
        linkTo:
        - box: lw-data-provider
          pin: server
    mq:
      subscribers:
        - name: messages
          attributes:
            - group
            - in
        - name: events
          attributes:
            - event
            - in
      publishers:
        - name: state
          attributes:
            - store
```

### Configuration example

This configuration is a general way for deploying components in th2.
It contains box configuration, pins' descriptions and other common parameters for a box.

Here is an example of configuration for component based on th2-processor-core-j (crawler mode):

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: my-processor
spec:
  custom-config:
    stateSessionAlias: my-processor-state
    enableStoreState: false
    useTransport: false

    crawler:
      from: 2021-06-16T12:00:00.00Z
      to: 2021-06-17T14:00:00.00Z
  
      intervalLength: PT10M
      syncInterval: PT10M
      awaitTimeout: 10
      awaitUnit: SECONDS
      
      messages:
        messageKinds: 
          - MESSAGE
          - RAW_MESSAGES
        bookToGroups:
          book1:
            - group1
            - group2
          book2:
            - group1
            - group2
      events:
        bookToScope:
          book3:
            - scope1
            - scope2
          book4:
            - scope1
            - scope2
    
    processorSettings:
      cacheCapacity: 100
      strategy: "single"
  pins:
    grpc:
      client:
        - name: to_data_provider
          service-class: com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
          linkTo:
            - box: lw-data-provider
              pin: server
        - name: to_data_provider_stream
          service-class: com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
          linkTo:
            - box: lw-data-provider
              pin: server
    mq:
      publishers:
        - name: state
          attributes:
            - store
```

Here is an example of configuration for component based on th2-processor-core-j (realtime mode):

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: my-processor
spec:
  custom-config:
    stateSessionAlias: my-processor-state
    enableStoreState: false

    realtime:
      enableMessageSubscribtion: true
      enableEventSubscribtion: false
    processorSettings:
      cacheCapacity: 100
      strategy: "single"
  pins:
    grpc:
      client:
        - name: to_data_provider
          service-class: com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
          linkTo:
            - box: lw-data-provider
              pin: server
    mq:
      subscribers:
        - name: messages
          attributes:
            - group
            - in
        - name: events
          attributes:
            - event
            - in
      publishers:
        - name: state
          attributes:
            - store
```

Here is an example of pins configuration for working in th2 transport mode:

```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2Box
metadata:
  name: my-processor
spec:
  customConfig:
    useTransport: true
  pins:
    grpc:
      client:
        - name: to_data_provider
          service-class: com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
          linkTo:
            - box: lw-data-provider
              pin: server
    mq:
      subscribers:
        - name: messages
          attributes:
            - transport-group
            - in
        - name: events
          attributes:
            - event
            - in
      publishers:
        - name: state
          attributes:
            - transport-group
```

# Release notes

## 0.3.0

### Feature:
+ Crawler strategy waits before processing interval if it ends in the future.
  Parameter `intervalPrecessingDelay` regulates how much time should pass after interval end to proceed with processing. 

## 0.2.0

### Feature:
+ Provided the mode for th2 transport working  

### Update:
+ bom: `4.5.0`
+ common: `5.7.2-dev`
+ common-utils: `2.2.2-dev`
+ grpc-lw-data-provider: `2.3.0-dev`