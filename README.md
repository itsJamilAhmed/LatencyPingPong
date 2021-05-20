# LatencyPingPong: Measure latency between applications connected through an event mesh

## What's in this repository?

The program in this repository lets you monitor the latency between two or more deployed instances, connected via a common [event mesh solution](https://solace.com/what-is-an-event-mesh/).

The programs may be distributed across different data centres, global regions or even multiple cloud providers. The event mesh is expected to take care of the connectivity, with the programs being used to validate the health and latency of that connectivity. This can be for the purposes of performance monitoring or enabling [cloud arbitrage](https://solace.com/blog/first-step-enabling-cloud-arbitrage/) for example.

## How does it work?

The program performs the role of:
1. Being a pinger (i.e. send a message to all available receivers)
2. Being a a ponger (i.e. respond to receiving a ping message and signal its presence). 

The same program can be simultaneously playing both roles or just do one role.

When in the role of the pinger, the program sends a ping message on a regular period and creates a results message for each successfully received pong message. That message will identify the ponger as well as calculate the round-trip latency between the pinger and ponger. At the end of one period, and before sending the next ping message, a summary results message is also created that details all the pong messages seen, the individual round-trip laency, as well as an arrival ranking of all pongs. 

When in the role of the ponger, the program simply listens for any ping messages and immedetiately reflects the same message back after adding identifying details about itself.

The ping message carries within it a high resolution timestamp, which returns back again to the original ponger inside the reflected pong message. That is what allows the pinger to calculate the elapsed round-trip time.  

## Event Mesh Setup

If the programs are being distributed across multiple locations, it is assumed the [event broker](https://solace.com/what-is-an-event-broker/) that each instance is connected to has been approriately configured to create connectivity between them in an [event mesh architecture](https://solace.com/what-is-an-event-mesh/).  

That setup assistance is not in scope of this readme. 

## Example deployment

This program is the basis of the Multi-Cloud Arbitrage demonstration that is available to view here:  
*https://london.solace.com/multi-cloud/arbitrage.html*  

![Sample Result](https://london.solace.com/multi-cloud/multi-cloud-arbitrage-example-result-1.png)


In that deployment a single pinger program is running on a 5-second ping period, with 9 simultaneous ponger instances deployed across the three regions of US-East, UK and Singapore, in the three public cloud providers of AWS, Azure and Google. 

### Node Names and Node Groups
The program has the concept of a node name and node-group to identify each pinger or ponger instance. In the default case, the node name is taken to be the hostname and the node-group is the value `Common`. This means that all pongers are in the same group and when their arrival ranking is determined for the Summary message, they are all in the same 'competition group'. 

In the example deployment above with 9 pongers, the pongers in each country are in competition with each other, but not with pongers in another country. Therefore it can be considered that there are three racing groups to configure, with node-group names of `US`, `UK` and `SG` selected used to represent the country they are deployed in.  

Then within each node-group, there are three nodes to represent the three cloud providers available per country. Therefore the node names are `AWS`, `Azure` and `Google`, repeated across the three node-groups of `US`, `UK` and `SG`. 

## Checking out and build instructions

To check out the project, clone this GitHub repository:

```
git clone https://github.com/itsJamilAhmed/LatencyPingPong
cd LatencyPingPong
```

Then using bundled Gradle wrapper build the source files to create an runnable jar file:
```
./gradlew build
cp ./build/libs/LatencyPingPong.jar .
```

Lastly run the jar and review the program help output to continue further:
```
java -jar LatencyPingPong.jar
```

## License

This project is licensed under the Apache License, Version 2.0. - See the [LICENSE](LICENSE) file for details.
