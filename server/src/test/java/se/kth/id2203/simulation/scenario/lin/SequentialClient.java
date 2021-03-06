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
package se.kth.id2203.simulation.scenario.lin;

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

import java.util.*;

/**
 * Test client that issues put/get in random order, used to verify linearizability among other things.
 *
 * @author Kim Hammar
 */
public class SequentialClient extends ComponentDefinition {

    final static Logger LOG = LoggerFactory.getLogger(SequentialClient.class);
    //******* Ports ******
    protected final Positive<Network> net = requires(Network.class);
    protected final Positive<Timer> timer = requires(Timer.class);
    //******* Fields ******
    private final NetAddress self = config().getValue("id2203.project.address", NetAddress.class);
    private final NetAddress server = config().getValue("id2203.project.bootstrap-address", NetAddress.class);
    private final SimulationResultMap res = SimulationResultSingleton.getInstance();
    private final Map<UUID, Operation> pending = new TreeMap<>();
    private final Random random = new Random();
    private int count = 0;
    //******* Handlers ******
    protected final Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            sendOp(new Operation("1", Integer.toString(random.nextInt(100)), "", Operation.OperationCode.PUT));

        }
    };
    protected final ClassMatchedHandler<OpResponse, Message> responseHandler = new ClassMatchedHandler<OpResponse, Message>() {

        @Override
        public void handle(OpResponse content, Message context) {
            LOG.debug("Got OpResponse: {}", content.toString());
            Operation operation = pending.remove(content.id);
            int messages = res.get("messages", Integer.class);
            if(count < messages){
                int opCode = random.nextInt(3);
                Operation.OperationCode operationCode = null;
                switch (opCode) {
                    case 0:
                        operationCode = Operation.OperationCode.GET;
                        break;
                    case 1:
                        operationCode = Operation.OperationCode.PUT;
                        break;
                    case 2:
                        operationCode = Operation.OperationCode.CAS;
                        break;
                }
                sendOp(new Operation(Integer.toString(random.nextInt(100)), Integer.toString(random.nextInt(100)), Integer.toString(random.nextInt(100)), operationCode));
                count++;
            }
        }
    };

    private void sendOp(Operation op) {
        RouteMsg rm = new RouteMsg(op.key, op); // don't know which partition is responsible, so ask the bootstrap server to forward it
        trigger(new Message(self, server, rm), net);
        pending.put(op.id, op);
        LOG.info("Sending {}", op.toString());
    }

    {
        subscribe(startHandler, control);
        subscribe(responseHandler, net);
    }
}
