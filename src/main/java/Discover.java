package com.java.JobController;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

class Discover {
    private static final AtomicInteger nextNodeId=new AtomicInteger();
    private static final ConcurrentHashMap<Integer, Node> nodes=new ConcurrentHashMap<>();
    private static final WorkerClientRegistry workers=new WorkerClientRegistry();

    public static class NodeManager extends WorkerToControllerGrpc.WorkerToControllerImplBase {
        @Override   
        public void registerNode(Node request,StreamObserver<Cnf> obs) {
            //Save in a in memory db
            Cnf.Builder builder=Cnf.newBuilder();
            if (request.getHostname().isBlank()
                    || request.getAgentEndpoint().isBlank()
                    || request.getCpu()<=0
                    || request.getMem()<=0) {

                obs.onNext(Cnf.newBuilder()
                        .setSuccess(false)
                        .setMessage("hostname, endpoint, CPU, and memory are required")
                        .build());
                obs.onCompleted();
                return;
            }
            int nodeId=nextNodeId.incrementAndGet();
            Node node=request.toBuilder()
                    .setId(nodeId)
                    .setState(NodeState.AVAILABLE)
                    .build();
            nodes.put(nodeId, node);
            try {
                workers.getOrConnect(nodeId, node.getAgentEndpoint());

                obs.onNext(Cnf.newBuilder()
                        .setSuccess(true)
                        .setNodeId(nodeId)
                        .build());
                obs.onCompleted();
            } catch (RuntimeException e) {
                nodes.remove(nodeId);
                obs.onNext(Cnf.newBuilder()
                        .setSuccess(false)
                        .setMessage("Could not connect to worker: " + e.getMessage())
                        .build());
                obs.onCompleted();
            }
        }
        
        @Override
        public void heartBeat(Node request,StreamObserver<Cnf> obs) {
            Node known=nodes.get(request.getId());
            if (known==null) {
                obs.onNext(Cnf.newBuilder()
                        .setSuccess(false)
                        .setMessage("Unknown node")
                        .build());
            }
            else {
                nodes.put(request.getId(), known.toBuilder()
                        .setState(request.getState())
                        .build());

                obs.onNext(Cnf.newBuilder().setSuccess(true).build());
            }
            obs.onCompleted();
        }
    }
    public static void main(String args[]) throws IOException,InterruptedException {
        Server server=ServerBuilder.forPort(9999)
                            .addService(new NodeManager())
                            .build();
        server.start();
        System.out.println("Broker started on port 9999");
        server.awaitTermination();
    }
}