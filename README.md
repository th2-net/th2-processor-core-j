# Description of th2-processor-core-j (0.0.1)

This is a common processor library which takes care of some features like requesting messages/events from a lw-data-provider (LDP), subscribing to message queues, verify incoming streams vs response from LDP, loading processor settings, etc.

# What is a processor?

The processor in th2 is a component that is responsible for handle th2 messages/events. Result of processing can be presented in any formats, for example, th2 events with analyze details, put data into external db, publish results via any external API.

# Configuration

Processor has three types of processing content: parsed message, raw message and event

## Processor core settings

Processor core settings can be specified in `custom-config` section. 

**stateSessionAlias: _my-processor-state_** - the processor uses this session alias to store and restore core and implementation parts of state **Required parameter when enableStoreState is true**

**enableStoreState: _false_** - the processor continues work after restart from the interval where it stopped before if this option is true otherwise the processor start from time specified by the **from** option.
The default value is **false**

**from: _2021-06-16T12:00:00.00Z_** - the lower boundary for processing interval of time.
The processor works on the data starting from this point in time. **Required parameter**

**to: _2021-06-17T14:00:00.00Z_** - the higher boundary for processing interval of time.
The processor does not work on the data after this point in time. **If it is not set the processor will work until it is stopped.**

**intervalLength: _PT10M_** - the step that the processor will use to create intervals.
It uses the Java Duration format. You can read more about it [here](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-).
The default value is **PT10M**.

**syncInterval: _PT10M_** - the processor uses this value for a request to lw-data-provider. lw-data-provider splits the whole requested interval to a synchronize intervals with specified length and publishes data for the whole requested books and scores / session aliases / group for each sync-interval.
It uses the Java Duration format. You can read more about it [here](https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-).
The default value is **PT10M**, also, the value should be less or equal as **intervalLength**.

**awaitTimeout: _10_** - the value of time wait interval for responds getting via MQ. The processor waits this time after lw-data-provider respond via gRPC until getting all requested data via MQ.
The default value is **10**.

**awaitUnit: _SECONDS_** - the time unit for **awaitTimeout** parameter.
Allowed values are described [here](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/temporal/ChronoUnit.html) in **Enum Constants** block.
The default value is **SECONDS**.

Processor can work of in one of two type of data: events, messages. The current mode depends on what of property are filled:

**messages** - processor will work on messages when this option is filled

**messageKind: _MESSAGE_** - the type of messages will be requested from lw-data-provider. The option supports the next values: MESSAGE, RAW_MESSAGE
The default value is **MESSAGE**.

**bookToGroups** - the combination of each **book** and **group** will be requested from lw-data-provider. This map must not be empty, also, each **group** collection can be empty or filled via not blank values. **Required parameter** 

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
    messages:
      messageKind: MESSAGE
      bookToGroups:
        book1:
          - group1
          - group2
        book2:
          - group1
          - group2
    stateSessionAlias: my-processor-state
    enableStoreState: false
      
    from: 2021-06-16T12:00:00.00Z
    to: 2021-06-17T14:00:00.00Z
      
    intervalLength: PT10M
    syncInterval: PT10M
    awaitTimeout: 10
    awaitUnit: SECONDS
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
    - name: to_data_provider
      connection-type: grpc-client
      service-class: com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
    - name: to_data_provider_stream
      connection-type: grpc-client
      service-class: com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
```

### Configuration example

This configuration is a general way for deploying components in th2.
It contains box configuration, pins' descriptions and other common parameters for a box.

Here is an example of configuration for component based on th2-processor-core-j:

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: my-processor
spec:
  custom-config:
    stateSessionAlias: my-processor-state
    enableStoreState: false

    from: 2021-06-16T12:00:00.00Z
    to: 2021-06-17T14:00:00.00Z

    intervalLength: PT10M
    syncInterval: PT10M
    awaitTimeout: 10
    awaitUnit: SECONDS
    
    messages:
      messageKind: MESSAGE
      bookToGroups:
        book1:
          - group1
          - group2
        book2:
          - group1
          - group2
    
    processorSettings:
      cacheCapacity: 100
      strategy: "single"
  pins:
    - name: to_data_provider
      connection-type: grpc-client
      service-class: com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
    - name: to_data_provider_stream
      connection-type: grpc-client
      service-class: com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
```