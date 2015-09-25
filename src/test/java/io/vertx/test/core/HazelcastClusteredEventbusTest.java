/*
 * Copyright 2014 Red Hat, Inc.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.test.core;

import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class HazelcastClusteredEventbusTest extends ClusteredEventBusTest {


    public static final int MESSAGE_TO_KILLED_NODE = 100500;

    static {
       // System.setProperty("hazelcast.wait.seconds.before.join", "1");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
    }


    private static final String ADDRESS = "some-address-";
    private static final int V_COUNT = 3;
    private static final int KILL_V_COUNT = 2;

    @Test
    public void testKillingSeveralNodeInSameTime() throws InterruptedException {
        startNodes(V_COUNT, new VertxOptions().setHAEnabled(false).setClusterPingInterval(100));

        CountDownLatch startVCounter = new CountDownLatch(V_COUNT-1);

        //register handlers for nodes except first
        for (int i = 1; i < V_COUNT; i++) {
            final int channel=i;
            String channelName = ADDRESS + i;
            System.out.println("register consumer at " + channelName);
            vertices[i].eventBus().consumer(channelName, msg -> {

                System.out.println(">> receive event " + msg.body() + ", in channel " + channel);
                msg.reply("ok" + msg.body());

                //KILLED NODE GET THIS MESSAGE
                if ((int)msg.body() == MESSAGE_TO_KILLED_NODE) {
                    fail("should not receive event after KILL!");
                }

            }).completionHandler(onSuccess(v -> startVCounter.countDown()));
        }

        //wait before registering process end
        startVCounter.await();

        //check that all verticle live
        for (int i = 1; i < V_COUNT; i++) {
            final int count = i;
            CountDownLatch checkVCounter = new CountDownLatch(1);

            vertices[0].runOnContext(v1 -> {
                EventBus ebSender = vertices[0].eventBus();
                System.out.println(">> send event " + count);
                ebSender.send(ADDRESS+count, count, reply -> {
                    if (reply.failed()) fail("Should receive ok");

                    assertEquals("ok"+count, reply.result().body());
                    checkVCounter.countDown();
                });
            });

            checkVCounter.await();
        }


        for (int i = 1; i <= KILL_V_COUNT; i++) {
            kill(i);
        }
        Thread.sleep(5000); //this sleep no sense. but wait anyway..

        System.out.println("TOTAL KILLED NODES: " + KILL_V_COUNT);

        //check that nodes is died
        CountDownLatch diedVCounter = new CountDownLatch(3);
        vertices[0].runOnContext(v1 -> {
            EventBus ebSender = vertices[0].eventBus();
            System.out.println(">>> send events to killed node ");

            for (int i = 1; i <= KILL_V_COUNT; i++) {
                ebSender.send(ADDRESS + i, MESSAGE_TO_KILLED_NODE, reply -> {
                    System.out.println(">>> receive from killed node " + reply.cause());
                    if (reply.succeeded()) fail("Should receive NO_HANDLER");

                    diedVCounter.countDown();
                });
            }

        });

        System.out.println("wait for event from killed node");
        diedVCounter.await();
        complete();
    }


    protected void kill(int pos) throws InterruptedException {
        VertxInternal v = (VertxInternal) vertices[pos];
        CountDownLatch killLatch = new CountDownLatch(1);
        v.executeBlocking(fut -> {
            v.simulateKill();
            fut.complete();
        }, ar -> {
            assertTrue(ar.succeeded());
            killLatch.countDown();
        });

        System.out.println("WAIT TO KILL " + pos);
        killLatch.await();
        System.out.println("NODE HAS BEEN KILLED " + pos);

    }

    @Override
    protected ClusterManager getClusterManager() {
        return new HazelcastClusterManager();
    }


}
