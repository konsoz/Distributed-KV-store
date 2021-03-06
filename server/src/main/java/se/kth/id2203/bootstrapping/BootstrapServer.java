/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.bootstrapping;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kth.id2203.bootstrapping.events.Boot;
import se.kth.id2203.bootstrapping.events.Booted;
import se.kth.id2203.bootstrapping.events.CheckIn;
import se.kth.id2203.bootstrapping.events.GetInitialAssignments;
import se.kth.id2203.bootstrapping.events.InitialAssignments;
import se.kth.id2203.bootstrapping.events.Ready;
import se.kth.id2203.bootstrapping.ports.Bootstrapping;
import se.kth.id2203.networking.Message;
import se.kth.id2203.networking.NetAddress;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * BootstrapServer, used by the first node in the cluster, hosts a type of handshake protocol that allows more servers
 * to join the cluster. Servers can check in at the bootstrap server and when enough servers are checked in the
 * bootstrapServer will generate a initial node-partition and then send it in a Boot-payload that commands the other
 * servers to "boot up" (booting in this context is starting the KV-store).
 */
public class BootstrapServer extends ComponentDefinition {

    final static Logger LOG = LoggerFactory.getLogger(BootstrapServer.class);
    //******* Ports ******
    protected final Negative<Bootstrapping> boot = provides(Bootstrapping.class);
    protected final Positive<Network> net = requires(Network.class);
    protected final Positive<Timer> timer = requires(Timer.class);
    //******* Fields ******
    final NetAddress self = config().getValue("id2203.project.address", NetAddress.class);
    final int bootThreshold = config().getValue("id2203.project.bootThreshold", Integer.class);
    final int replicationDegree = config().getValue("id2203.project.replicationDegree", Integer.class);
    final int keySpace = config().getValue("id2203.project.keySpace", Integer.class);
    private State state = State.COLLECTING;
    private UUID timeoutId;
    private final Set<NetAddress> active = new HashSet<>();
    private final Set<NetAddress> ready = new HashSet<>();
    private NodeAssignment initialAssignment = null;
    //******* Handlers ******

    /**
     * Startup, setup timer for periodically checking if enough servers have checked in to intialize bootup
     */
    protected final Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start e) {
            LOG.info("Starting bootstrap server on {}, waiting for {} nodes...", self, bootThreshold);
            long timeout = (config().getValue("id2203.project.keepAlivePeriod", Long.class) * 2);
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(timeout, timeout);
            spt.setTimeoutEvent(new BSTimeout(spt));
            trigger(spt, timer);
            timeoutId = spt.getTimeoutEvent().getTimeoutId();
            active.add(self);
        }
    };
    /**
     * Check if enough servers have checked in to bootup
     */
    protected final Handler<BSTimeout> timeoutHandler = new Handler<BSTimeout>() {
        @Override
        public void handle(BSTimeout e) {
            if (state == State.COLLECTING) {
                LOG.info("{} hosts in active set.", active.size());
                if (active.size() >= bootThreshold) {
                    bootUp();
                }
            } else if (state == State.SEEDING) {
                LOG.info("{} hosts in ready set.", ready.size());
                if (ready.size() >= bootThreshold) {
                    LOG.info("Finished seeding. Bootstrapping complete.");
                    trigger(new Booted(initialAssignment, new HashMap<Integer, String>()), boot);
                    state = State.DONE;
                }
            } else if (state == State.DONE) {
                suicide();
            }
        }
    };
    /**
     * Received initial partition-assignment from VSOverlayManager
     */
    protected final Handler<InitialAssignments> assignmentHandler = new Handler<InitialAssignments>() {
        @Override
        public void handle(InitialAssignments e) {
            LOG.info("Seeding assignments...");
            initialAssignment = e.assignment;
            for (NetAddress node : active) {
                trigger(new Message(self, node, new Boot(initialAssignment, new HashMap<Integer, String>())), net);
            }
            ready.add(self);
        }
    };

    /**
     * Received checkIn from bootstrapClient, add it to the set
     */
    protected final ClassMatchedHandler<CheckIn, Message> checkinHandler = new ClassMatchedHandler<CheckIn, Message>() {

        @Override
        public void handle(CheckIn content, Message context) {
            active.add(content.netaddress);
        }
    };

    /**
     * Received ready-request from bootstrap client, impliying that the client have booted successfully.
     */
    protected final ClassMatchedHandler<Ready, Message> readyHandler = new ClassMatchedHandler<Ready, Message>() {
        @Override
        public void handle(Ready content, Message context) {
            ready.add(context.getSource());
        }
    };

    /**
     * Kompics "instance initializer", subscribe handlers to ports.
     */
    {
        subscribe(startHandler, control);
        subscribe(timeoutHandler, timer);
        subscribe(assignmentHandler, boot);
        subscribe(checkinHandler, net);
        subscribe(readyHandler, net);
    }

    /**
     * Bootup complete
     */
    @Override
    public void tearDown() {
        trigger(new CancelPeriodicTimeout(timeoutId), timer);
        LOG.debug("Bootstrap complete");
    }

    /**
     * Initialize bootup by requesting initial partition assignment from VSOverlayManager
     */
    private void bootUp() {
        LOG.info("Threshold reached. Generating assignments...");
        state = State.SEEDING;
        trigger(new GetInitialAssignments(ImmutableSet.copyOf(active), replicationDegree, keySpace), boot);
    }

    static enum State {

        COLLECTING,
        SEEDING,
        DONE;
    }
}
