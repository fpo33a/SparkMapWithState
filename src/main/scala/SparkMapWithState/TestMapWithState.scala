/* in order to test:

1/ starts a socket server app that sends some data 

1.1/ server code
#!/usr/bin/python3           # This is server.py file
import socket
import time

# create a socket object
serversocket = socket.socket(
	        socket.AF_INET, socket.SOCK_STREAM)

# get local machine name
host = "localhost"

port = 9999

# bind to the port
serversocket.bind((host, port))

# queue up to 5 requests
serversocket.listen(5)

while True:
   # establish a connection
   clientsocket,addr = serversocket.accept()

   print("Got a connection from %s" % str(addr))

   msg = 'Thank you for connecting frank. This is the connecting for test. This is hello'+ "\r\n"  +' hello frank'  + "\r\n"  +' hello jean'  + "\r\n"
   clientsocket.send(msg.encode('ascii'))

   time.sleep (10)
   msg = 'this is another batch '+ "\r\n"  +' Thank you frank'  + "\r\n"  +' and jean'  + "\r\n"
   clientsocket.send(msg.encode('ascii'))

   time.sleep (15)

   msg = 'this is roro and riri\r\n'
   clientsocket.send(msg.encode('ascii'))

#   clientsocket.close()

1.2/ execution

  C:\windows\system32>cd \frank\SparkMapWithState
  
  C:\frank\SparkMapWithState>python server.py
  Got a connection from ('127.0.0.1', 63922)

2/ run the spark job

2.1/ set the environment

  C:\frank\SparkMapWithState\target>dir c:\temp\bin
   Le volume dans le lecteur C s'appelle TI31128200B
   Le num�ro de s�rie du volume est F063-B02B
  
   R�pertoire de c:\temp\bin
  
  08-06-19  19:11    <DIR>          .
  08-06-19  19:11    <DIR>          ..
  08-06-19  19:08            46.952 winutils.exe
                 1 fichier(s)           46.952 octets
                 2 R�p(s)  262.817.726.464 octets libres
  
  C:\frank\SparkMapWithState\target>
  C:\frank\SparkMapWithState\target>set HADOOP_HOME=c:\temp

2.2/ build spark jar
  mvn clean package

2.3/ run the process (send 'println' result in 'a.txt' file
  C:\frank\SparkMapWithState\target>java -jar SparkMapWithState-1.0-SNAPSHOT-jar-with-dependencies.jar > a.txt
  Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
  19/06/09 08:05:11 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
  19/06/09 08:05:11 INFO SparkContext: Running Spark version 2.3.0
  19/06/09 08:05:11 INFO SparkContext: Submitted application: TestMapWithStateJob
  19/06/09 08:05:12 INFO SecurityManager: Changing view acls to: frank
  19/06/09 08:05:12 INFO SecurityManager: Changing modify acls to: frank
  19/06/09 08:05:12 INFO SecurityManager: Changing view acls groups to:
  19/06/09 08:05:12 INFO SecurityManager: Changing modify acls groups to:
  19/06/09 08:05:12 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(frank); groups with view permissions: Set(); users  with modify permissions: Se
  19/06/09 08:05:12 INFO Utils: Successfully started service 'sparkDriver' on port 63907.
  19/06/09 08:05:13 INFO SparkEnv: Registering MapOutputTracker
  19/06/09 08:05:13 INFO SparkEnv: Registering BlockManagerMaster
  19/06/09 08:05:13 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
  19/06/09 08:05:13 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
  19/06/09 08:05:13 INFO DiskBlockManager: Created local directory at C:\Users\frank\AppData\Local\Temp\blockmgr-76b2680d-c1d0-42ab-b1b9-daf863179060
  19/06/09 08:05:13 INFO MemoryStore: MemoryStore started with capacity 896.4 MB
  19/06/09 08:05:13 INFO SparkEnv: Registering OutputCommitCoordinator
  19/06/09 08:05:15 INFO Utils: Successfully started service 'SparkUI' on port 4040.
  19/06/09 08:05:15 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://PC-CORENTIN:4040
  19/06/09 08:05:16 INFO Executor: Starting executor ID driver on host localhost
  19/06/09 08:05:16 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 63918.
  19/06/09 08:05:16 INFO NettyBlockTransferService: Server created on PC-CORENTIN:63918
  19/06/09 08:05:16 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
  19/06/09 08:05:16 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, PC-CORENTIN, 63918, None)
  19/06/09 08:05:16 INFO BlockManagerMasterEndpoint: Registering block manager PC-CORENTIN:63918 with 896.4 MB RAM, BlockManagerId(driver, PC-CORENTIN, 63918, None)
  19/06/09 08:05:16 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, PC-CORENTIN, 63918, None)
  19/06/09 08:05:16 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, PC-CORENTIN, 63918, None)
  19/06/09 08:05:23 WARN RandomBlockReplicationPolicy: Expecting 1 replicas with only 0 peer/s.
  19/06/09 08:05:23 WARN BlockManager: Block input-0-1560060323600 replicated to only 0 peer(s) instead of 1 peers
  19/06/09 08:05:33 WARN RandomBlockReplicationPolicy: Expecting 1 replicas with only 0 peer/s.
  19/06/09 08:05:33 WARN BlockManager: Block input-0-1560060333600 replicated to only 0 peer(s) instead of 1 peers
  19/06/09 08:05:48 WARN RandomBlockReplicationPolicy: Expecting 1 replicas with only 0 peer/s.
  19/06/09 08:05:48 WARN BlockManager: Block input-0-1560060348600 replicated to only 0 peer(s) instead of 1 peers
  
2.4/ stop (via CTRL-C)

  19/06/09 08:11:11 WARN SocketReceiver: Error receiving data
  java.net.SocketException: Socket closed
          at java.net.SocketInputStream.socketRead0(Native Method)
          at java.net.SocketInputStream.socketRead(Unknown Source)
          at java.net.SocketInputStream.read(Unknown Source)
          at java.net.SocketInputStream.read(Unknown Source)
          at sun.nio.cs.StreamDecoder.readBytes(Unknown Source)
          at sun.nio.cs.StreamDecoder.implRead(Unknown Source)
          at sun.nio.cs.StreamDecoder.read(Unknown Source)
          at java.io.InputStreamReader.read(Unknown Source)
          at java.io.BufferedReader.fill(Unknown Source)
          at java.io.BufferedReader.readLine(Unknown Source)
          at java.io.BufferedReader.readLine(Unknown Source)
          at org.apache.spark.streaming.dstream.SocketReceiver$$anon$1.getNext(SocketInputDStream.scala:121)
          at org.apache.spark.streaming.dstream.SocketReceiver$$anon$1.getNext(SocketInputDStream.scala:119)
          at org.apache.spark.util.NextIterator.hasNext(NextIterator.scala:73)
          at org.apache.spark.streaming.dstream.SocketReceiver.receive(SocketInputDStream.scala:91)
          at org.apache.spark.streaming.dstream.SocketReceiver$$anon$2.run(SocketInputDStream.scala:72)
  19/06/09 08:11:11 ERROR ReceiverTracker: Deregistered receiver for stream 0: Stopped by driver
  19/06/09 08:11:11 WARN ReceiverSupervisorImpl: Restarting receiver with delay 2000 ms: Error receiving data
  java.net.SocketException: Socket closed
          at java.net.SocketInputStream.socketRead0(Native Method)
          at java.net.SocketInputStream.socketRead(Unknown Source)
          at java.net.SocketInputStream.read(Unknown Source)
          at java.net.SocketInputStream.read(Unknown Source)
          at sun.nio.cs.StreamDecoder.readBytes(Unknown Source)
          at sun.nio.cs.StreamDecoder.implRead(Unknown Source)
          at sun.nio.cs.StreamDecoder.read(Unknown Source)
          at java.io.InputStreamReader.read(Unknown Source)
          at java.io.BufferedReader.fill(Unknown Source)
          at java.io.BufferedReader.readLine(Unknown Source)
          at java.io.BufferedReader.readLine(Unknown Source)
          at org.apache.spark.streaming.dstream.SocketReceiver$$anon$1.getNext(SocketInputDStream.scala:121)
          at org.apache.spark.streaming.dstream.SocketReceiver$$anon$1.getNext(SocketInputDStream.scala:119)
          at org.apache.spark.util.NextIterator.hasNext(NextIterator.scala:73)
          at org.apache.spark.streaming.dstream.SocketReceiver.receive(SocketInputDStream.scala:91)
          at org.apache.spark.streaming.dstream.SocketReceiver$$anon$2.run(SocketInputDStream.scala:72)
  19/06/09 08:11:11 WARN ReceiverSupervisorImpl: Receiver has been stopped
  Exception in thread "receiver-supervisor-future-0" java.lang.Error: java.lang.InterruptedException: sleep interrupted
          at java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)
          at java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)
          at java.lang.Thread.run(Unknown Source)
  Caused by: java.lang.InterruptedException: sleep interrupted
          at java.lang.Thread.sleep(Native Method)
          at org.apache.spark.streaming.receiver.ReceiverSupervisor$$anonfun$restartReceiver$1.apply$mcV$sp(ReceiverSupervisor.scala:196)
          at org.apache.spark.streaming.receiver.ReceiverSupervisor$$anonfun$restartReceiver$1.apply(ReceiverSupervisor.scala:189)
          at org.apache.spark.streaming.receiver.ReceiverSupervisor$$anonfun$restartReceiver$1.apply(ReceiverSupervisor.scala:189)
          at scala.concurrent.impl.Future$PromiseCompletingRunnable.liftedTree1$1(Future.scala:24)
          at scala.concurrent.impl.Future$PromiseCompletingRunnable.run(Future.scala:24)
          ... 3 more
  19/06/09 08:11:12 WARN BatchedWriteAheadLog: BatchedWriteAheadLog Writer queue interrupted.
  
  C:\frank\SparkMapWithState\target>

2.5/ check outputs in 'a.txt'

  ****** line = [org.apache.spark.streaming.dstream.SocketInputDStream@34cf5a97]
  -------------------------------------------
  Time: 1560060320000 ms
  -------------------------------------------
  
  **** Updating key [connecting] sum = 1
  **** Updating key [frank.] sum = 1
  **** Updating key [connecting] sum = 2
  **** Updating key [test.] sum = 1
  **** Updating key [] sum = 1
  **** Updating key [] sum = 2
  **** Updating key [jean] sum = 1
  **** Updating key [for] sum = 1
  **** Updating key [the] sum = 1
  **** Updating key [for] sum = 2
  **** Updating key [Thank] sum = 1
  **** Updating key [This] sum = 1
  **** Updating key [is] sum = 1
  **** Updating key [This] sum = 2
  **** Updating key [is] sum = 2
  **** Updating key [hello] sum = 1
  **** Updating key [hello] sum = 2
  **** Updating key [frank] sum = 1
  **** Updating key [hello] sum = 3
  -------------------------------------------
  Time: 1560060325000 ms
  -------------------------------------------
  Some((connecting,1))
  Some((frank.,1))
  Some((connecting,2))
  Some((test.,1))
  Some((,1))
  Some((,2))
  Some((jean,1))
  Some((for,1))
  Some((the,1))
  Some((for,2))
  ...
  
  **** Updating key [you] sum = 1
  -------------------------------------------
  Time: 1560060330000 ms
  -------------------------------------------
  
  **** Updating key [] sum = 3
  **** Updating key [] sum = 4
  **** Updating key [jean] sum = 2
  **** Updating key [this] sum = 1
  **** Updating key [is] sum = 3
  **** Updating key [batch] sum = 1
  **** Updating key [Thank] sum = 2
  **** Updating key [frank] sum = 2
  **** Updating key [another] sum = 1
  **** Updating key [you] sum = 2
  **** Updating key [and] sum = 1
  -------------------------------------------
  Time: 1560060335000 ms
  -------------------------------------------
  Some((,3))
  Some((,4))
  Some((jean,2))
  Some((this,1))
  Some((is,3))
  Some((batch,1))
  Some((Thank,2))
  Some((frank,2))
  Some((another,1))
  Some((you,2))
  ...
  
  -------------------------------------------
  Time: 1560060340000 ms
  -------------------------------------------
  
  -------------------------------------------
  Time: 1560060345000 ms
  -------------------------------------------
  
  **** Updating key [this] sum = 2
  **** Updating key [is] sum = 4
  **** Updating key [roro] sum = 1
  **** Updating key [riri] sum = 1
  **** Updating key [and] sum = 2
  -------------------------------------------
  Time: 1560060350000 ms
  -------------------------------------------
  Some((this,2))
  Some((is,4))
  Some((roro,1))
  Some((riri,1))
  Some((and,2))
  
  -------------------------------------------
  Time: 1560060355000 ms
  -------------------------------------------
  
  -------------------------------------------
  Time: 1560060360000 ms
  -------------------------------------------
  
  -------------------------------------------
  Time: 1560060365000 ms
  -------------------------------------------
  
  -------------------------------------------
  Time: 1560060370000 ms
  -------------------------------------------
  
  **** [test.] key is timing out...will be removed.
  **** [connecting] key is timing out...will be removed.
  **** [] key is timing out...will be removed.
  **** [jean] key is timing out...will be removed.
  **** [frank.] key is timing out...will be removed.
  **** [for] key is timing out...will be removed.
  **** [the] key is timing out...will be removed.
  **** [This] key is timing out...will be removed.
  **** [hello] key is timing out...will be removed.
  **** [Thank] key is timing out...will be removed.
  **** [batch] key is timing out...will be removed.
  **** [frank] key is timing out...will be removed.
  **** [you] key is timing out...will be removed.
  **** [another] key is timing out...will be removed.
  -------------------------------------------
  Time: 1560060375000 ms
  -------------------------------------------
  Some((test.,1))
  Some((connecting,2))
  Some((,4))
  Some((jean,2))
  Some((frank.,1))
  Some((for,2))
  Some((the,1))
  Some((This,2))
  Some((hello,3))
  Some((Thank,2))
  ...
  
  -------------------------------------------
  Time: 1560060380000 ms
  -------------------------------------------
  
  -------------------------------------------
  Time: 1560060385000 ms
  -------------------------------------------
  
  -------------------------------------------
  Time: 1560060390000 ms
  -------------------------------------------
  
  **** [this] key is timing out...will be removed.
  **** [is] key is timing out...will be removed.
  **** [riri] key is timing out...will be removed.
  **** [roro] key is timing out...will be removed.
  **** [and] key is timing out...will be removed.
  -------------------------------------------
  Time: 1560060395000 ms
  -------------------------------------------
  Some((this,2))
  Some((is,4))
  Some((riri,1))
  Some((roro,1))
  Some((and,2))
  
  -------------------------------------------
  Time: 1560060400000 ms
  -------------------------------------------
  
  ...
*/

package SparkMapWithState

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, State, StateSpec }

/**
  * Listens to socket text stream on host=localhost, port=9999.
  * Tokenizes the incoming stream into (words, no. of occurrences) and tracks the state of the word using the API 'mapWithState'.
  * Keys with no updates are removed using StateSpec.timeout API.
  * Chekpointing frequency every 20s.
  */

object TestMapWithState {
  //val checkpointDir: String = "hdfs://localhost:9000/user/frank/spark-chkpt"
  val checkpointDir: String = "c://frank//spark-chkpt"

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(checkpointDir, createFunc)


    ssc.start()
    ssc.awaitTermination()
  }

  def createFunc(): StreamingContext = {
    val ssc = new StreamingContext(new SparkConf().setAppName("TestMapWithStateJob").setMaster("local[2]"),
      Seconds(5))

    ssc.checkpoint(checkpointDir)
    ssc.sparkContext.setLogLevel("WARN")

    // State specs
    val stateSpec = StateSpec.function(mappingFunc _)
      .numPartitions(4)
      .timeout(Seconds(30)) // idle keys will be removed.

    val line = ssc.socketTextStream("localhost", "9999".toInt)
    println ("****** line = ["+line+"]")

    line.flatMap(_.split(" "))
      .map( (_, 1) )
      .mapWithState(stateSpec)
      .checkpoint(Seconds(20))
      .print()

    ssc
  }

  /**
    * Mapping function for the 'mapWithState' API.
    */
  def mappingFunc(key: String, value: Option[Int], state: State[Int]): Option[(String, Int)] = {
    val sum = value.getOrElse(0) + state.getOption().getOrElse(0)

    // updating the state of non-idle keys...
    // To call State.update(...) we need to check State.isTimingOut() == false,
    // else there will be NoSuchElementException("Cannot update the state that is timing out")
    if (state.isTimingOut())
      println("**** ["+ key + "] key is timing out...will be removed.")
    else
    {
      println ("**** Updating key [" + key + "] sum = "+sum)
      state.update(sum)
    }


    Some((key, sum))
  }

}