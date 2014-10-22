Countdera
=========

Countdera is a Javascript MapReduce platform that runs in the browser in the place of ads. 
It's drag-and-drop, distributed, and fault-tolerant under crash failures.

We built Countdera on top of Firebase and used two node servers (written in coffeescript) to manage jobs and nodes in the cluster.

We wrote all of this code for [HackTX 2014](https://www.hackerleague.org/hackathons/hacktx-2014/hacks/countdera) in 24 hours
where we made it to the finals but didn't place.

## Concept
The idea behind Countdera was to allow webpages to serve an iframe that would use a viewer's CPU cycles (after they opt-in)
to run MapReduce jobs and generate revenue for the hosting website. Researchers or companies would then come to our site
and pay to use our massive cluster of these worker nodes to run their computations. This system would have the potential to
replace online advertising and was inspired by Tidbit.

The protocol we built Countdera on was inspired by MapReduce but was our own design. It makes some simplifying assumptions
but is generally fully-featured. You can read more about the design of the protocol that this repo implements on [this website](http://dwett.com/demo).

## Running this as is for yourself
- Create a Firebase and put the name of your Firebase in `/src/config.coffee`
- Create the directory `/countdera/output` on the machine you're running the server on.
- Run `npm install` in the root of the project.
- Run `npm start` to start the job tracking server and the static webserver on `http://localhost:3000`
- Run `node bin/ioserver/ioserver.js` to start the server that creates the output files
- Visit `http://localhost:3000` and select to be a worker (a node in the cluster) or make a MapReduce job.
- Embed `http://your_domain/worker` in an embedded iframe for the workers.

Note: Your input files but be available cross domain. This means when you server them you must send the proper CORS headers.

### Things that would need to be done to make this production worthy:
- Batching map-output messages. Right now each emission from a mapper is sent to every reducer. For large input files,
this would require many millions of messages.
- Possibly using WebRTC instead of Firebase FIFO queues for messages (We didn't do this because most of the WebRTC libraries suck).
- Firebase permissions. Currently people could do bad things to the cluster if they knew the Firebase name.
