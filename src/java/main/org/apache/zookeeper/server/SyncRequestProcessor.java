/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * 负责把写request持久化到本地磁盘，为了提高写磁盘的效率，这里使用的是缓冲写，但是会周期性（1000个request）的调用flush操作，
 * flush之后request已经确保写到磁盘了，这时会把请求传给AckRequestProcessor继续处理
 */
public class SyncRequestProcessor extends Thread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    // Zookeeper服务器
    private final ZooKeeperServer zks;
    // 请求队列，大量的事物消息都会先进入到该队列
    private final LinkedBlockingQueue<Request> queuedRequests =
        new LinkedBlockingQueue<Request>();
    // 下个处理器
    private final RequestProcessor nextProcessor;

    // 快照处理线程
    private Thread snapInProcess = null;
    // 是否在运行中
    volatile private boolean running;

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     *
     */
    // 等待被刷新到磁盘的请求队列
    private final LinkedList<Request> toFlush = new LinkedList<Request>();
    // 随机数生成器
    private final Random r = new Random(System.nanoTime());
    /**
     * The number of log entries to log before starting a snapshot
     */
    // 快照个数
    private static int snapCount = ZooKeeperServer.getSnapCount();

    private final Request requestOfDeath = Request.requestOfDeath;

    public SyncRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor)
    {
        super("SyncThread:" + zks.getServerId());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    @Override
    public void run() {
        try {
            // 写日志数量初始化为0
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            // 确保所有的服务器在同一时间不是使用的同一个快照
            int randRoll = r.nextInt(snapCount/2);
            while (true) {
                Request si = null;
                // 没有需要刷新到磁盘的请求
                if (toFlush.isEmpty()) {
                    // 从请求队列中取出一个请求，若队列为空也就是没有接收到事物消息则会阻塞
                    si = queuedRequests.take();
                } else {
                    //一旦toFlush中有数据需要刷新到磁盘
                    //则从queuedRequests队列中非阻塞的获取数据，继续后续的写磁盘操作
                    //当queuedRequests队列为空，也就是没有事物消息后，直接执行磁盘刷新操作
                    si = queuedRequests.poll();
                    if (si == null) {
                        flush(toFlush);
                        continue;
                    }
                }
                // 在关闭处理器之后，会添加requestOfDeath，表示关闭后不再处理请求
                if (si == requestOfDeath) {
                    break;
                }
                // 处理事物请求
                if (si != null) {
                    // track the number of records written to the log
                    // 将事物数据通过写缓冲追加到日志文件，这里只有事务性请求才会返回true
                    if (zks.getZKDatabase().append(si)) {
                        // 写入一条日志，logCount加1
                        logCount++;
                        // 满足roll the log的条件
                        // 每隔snapCount/2个request会重新生成一个snapshot并滚动一次txnlog，
                        // 同时为了避免所有的zookeeper server在同一个时间生成snapshot和滚动日志，这里会再加上一个随机数，snapCount的默认值是10w个request
                        if (logCount > (snapCount / 2 + randRoll)) {
                            randRoll = r.nextInt(snapCount/2);
                            // roll the log
                            //将当前日志文件刷新到磁盘，并重新开启一个新的日志文件
                            zks.getZKDatabase().rollLog();
                            // take a snapshot
                            //判断生成快照的线程是否正在执行
                            if (snapInProcess != null && snapInProcess.isAlive()) {
                                LOG.warn("Too busy to snap, skipping");
                            } else {
                                //开启线程负责生成快照的线程
                                snapInProcess = new Thread("Snapshot Thread") {
                                        public void run() {
                                            try {
                                                //开启新的线程生成快照文件
                                                zks.takeSnapshot();
                                            } catch(Exception e) {
                                                LOG.warn("Unexpected exception", e);
                                            }
                                        }
                                    };
                                snapInProcess.start();
                            }
                            // 重置为0
                            logCount = 0;
                        }
                    } else if (toFlush.isEmpty()) {
                        // 优化读请求，直接交给下一个处理器
                        // optimization for read heavy workloads
                        // iff this is a read, and there are no pending
                        // flushes (writes), then just pass this to the next
                        // processor
                        nextProcessor.processRequest(si);
                        if (nextProcessor instanceof Flushable) {
                            // 刷新到磁盘
                            ((Flushable)nextProcessor).flush();
                        }
                        // 跳过后续处理
                        continue;
                    }
                    //将请求添加至被刷新至磁盘的队列
                    toFlush.add(si);
                    //当有不断的事物请求发送过来，每隔1000条数据执行一次刷盘
                    if (toFlush.size() > 1000) {
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            LOG.error("Severe unrecoverable error, exiting", t);
            running = false;
            System.exit(11);
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    private void flush(LinkedList<Request> toFlush)
        throws IOException, RequestProcessorException
    {
        if (toFlush.isEmpty())
            return;

        //磁盘事物数据刷盘
        zks.getZKDatabase().commit();
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();
            //都已经刷入磁盘后才执行下一个处理器
            nextProcessor.processRequest(i);
        }
        if (nextProcessor instanceof Flushable) {
            ((Flushable)nextProcessor).flush();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(requestOfDeath);
        try {
            if(running){
                this.join();
            }
        } catch(InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        }
        nextProcessor.shutdown();
    }

    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        queuedRequests.add(request);
    }

}
