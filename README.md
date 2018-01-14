# Apache Spark Receiver for AMPS
This is a sample Apache Spark receiver for AMPS messaging bus written in Scala.

## Usage
Receiver connects to local instance of Spark and remote AMPS instance and start publishing word count statistics inside AMPS messages every second.

It takes two command line arguments (see code for more configuration options):

__AMPSSparkReceiver "AMPS server" "AMPS topic"__



### Example

Use AMPS command line utility to publish a few mmesages:
```
spark publish -server 34.201.116.96:9007 -type json -topic test

hi
friend
friend

```

On the receiver end, receiver will print statistics gathered in Spark:

```
AMPSSparkReceiver "tcp://34.201.116.96:9007/amps/json" "test"


Time: 1515903440000 ms
-------------------------------------------
(hi,1)
(friend,2)

-------------------------------------------
Time: 1515903441000 ms
-------------------------------------------

-------------------------------------------
Time: 1515903442000 ms
-------------------------------------------
```