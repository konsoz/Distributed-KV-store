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
package se.kth.id2203.simulation.scenario.ops;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.id2203.kvstore.OpResponse;
import se.kth.id2203.kvstore.Operation;
import se.kth.id2203.networking.Message;
import se.kth.id2203.networking.NetAddress;
import se.kth.id2203.overlay.RouteMsg;
import se.kth.id2203.simulation.result.SimulationResultMap;
import se.kth.id2203.simulation.result.SimulationResultSingleton;
import se.sics.kompics.*;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Simple test client that issues operations to the store.
 *
 * @author Kim Hammar
 */
public class SimpleOpsScenarioClient extends ComponentDefinition {

    final static Logger LOG = LoggerFactory.getLogger(SimpleOpsScenarioClient.class);
    //******* Ports ******
    protected final Positive<Network> net = requires(Network.class);
    protected final Positive<Timer> timer = requires(Timer.class);
    //******* Fields ******
    private final NetAddress self = config().getValue("id2203.project.address", NetAddress.class);
    private final NetAddress server = config().getValue("id2203.project.bootstrap-address", NetAddress.class);
    private final SimulationResultMap res = SimulationResultSingleton.getInstance();
    private final Map<UUID, Operation> pending = new TreeMap<>();
    private boolean get = false;
    private boolean cas1 = false;
    private boolean cas2 = false;
    //******* Handlers ******
    protected final Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            int messages = res.get("messages", Integer.class);
            int fourth = messages / 4;
            for (int i = 0; i < fourth; i++) {
                sendPut(i);
            }
        }
    };
    protected final ClassMatchedHandler<OpResponse, Message> responseHandler = new ClassMatchedHandler<OpResponse, Message>() {

        @Override
        public void handle(OpResponse content, Message context) {
            LOG.debug("Got OpResponse: {}", content.toString());
            Operation operation = pending.remove(content.id);
            LOG.debug("PendingOp was: " + operation.toString());
            if (operation.key != null) {
                if (operation.operationCode == Operation.OperationCode.CAS) {
                    if (!cas2)
                        res.put(operation.operationCode + "1-" + operation.key, content.status.toString() + " - " + content.value);
                    else
                        res.put(operation.operationCode + "2-" + operation.key, content.status.toString() + " - " + content.value);
                } else
                    res.put(operation.operationCode + "-" + operation.key, content.status.toString() + " - " + content.value);
            } else {
                LOG.warn("ID {} was not pending! Ignoring response.", content.id);
            }
            int messages = res.get("messages", Integer.class);
            int fourth = messages / 4;
            if (pending.isEmpty() && !get) {
                for (int i = 0; i < fourth; i++) {
                    sendGet(i);
                }
                get = true;
            }
            if (get && pending.isEmpty() && !cas1) {
                for (int i = 0; i < fourth; i++) {
                    if ((i % 2) == 0)
                        sendCas("1", i, i, i + 1);
                    else
                        sendCas("1", i, -1, -100);
                }
                cas1 = true;
            }
            if (cas1 && !cas2 && pending.isEmpty()) {

                for (int i = 0; i < fourth; i++) {
                    sendCas("2", i, i, i);
                }
                cas2 = true;
            }
        }
    };

    private void sendCas(String prefix, int key, int ref, int val) {
        Operation op = new Operation("test" + key, Integer.toString(val), Integer.toString(ref), Operation.OperationCode.CAS);
        RouteMsg rm = new RouteMsg(op.key, op); // don't know which partition is responsible, so ask the bootstrap server to forward it
        trigger(new Message(self, server, rm), net);
        pending.put(op.id, op);
        LOG.info("Sending {}", op.toString());
        res.put(op.operationCode + prefix + "-" + op.key, "SENT");
    }

    private void sendPut(int i) {
        Operation op = new Operation("test" + i, Integer.toString(i), "", Operation.OperationCode.PUT);
        RouteMsg rm = new RouteMsg(op.key, op); // don't know which partition is responsible, so ask the bootstrap server to forward it
        trigger(new Message(self, server, rm), net);
        pending.put(op.id, op);
        LOG.info("Sending {}", op.toString());
        res.put(op.operationCode + "-" + op.key, "SENT");
    }

    private void sendGet(int i) {
        Operation op = new Operation("test" + i, "", "", Operation.OperationCode.GET);
        RouteMsg rm = new RouteMsg(op.key, op); // don't know which partition is responsible, so ask the bootstrap server to forward it
        trigger(new Message(self, server, rm), net);
        pending.put(op.id, op);
        LOG.info("Sending {}", op.toString());
        res.put(op.operationCode + "-" + op.key, "SENT");
    }

    {
        subscribe(startHandler, control);
        subscribe(responseHandler, net);
    }
}
