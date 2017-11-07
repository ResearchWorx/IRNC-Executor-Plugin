package edu.uky.irnc.executor.helpers;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import edu.uky.irnc.executor.Plugin;

import java.io.IOException;
import java.util.*;

public class Runner implements Runnable {
    private static Set<String> executables = new HashSet<>(Arrays.asList("netflow", "packet_trace",
            "packet_validation", "sendudp", "kanon",
            "perfSONAR_Throughput", "amis_argus",
            "ls", "/home/acanets/AMIS/amis"));
    private Plugin plugin;
    private CLogger logger;
    private String command;
    private String exchangeID;
    private String dstRegion;
    private String dstAgent;
    private String dstPlugin;
    private boolean running = false;
    private boolean complete = false;

    public Runner(Plugin plugin, String command, String exchangeID,
                  String dstRegion, String dstAgent, String dstPlugin) {
        this.plugin = plugin;
        this.logger = new CLogger(Runner.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(),
                plugin.getPluginID(), CLogger.Level.Trace);
        this.command = command;
        this.exchangeID = exchangeID;
        this.dstRegion = dstRegion;
        this.dstAgent = dstAgent;
        this.dstPlugin = dstPlugin;
    }

    @Override
    public void run() {
        try {
            logger.info("Running Runner");
            logger.debug("Command: [" + command + "]");
            boolean canRun = false;
            logger.trace("Checking to see if eligable for running");
            for (String executable : executables)
                if (command.startsWith(executable))
                    canRun = true;
            logger.debug("canRun = {}", canRun);
            if (!canRun) return;
            running = true;
            logger.trace("Setting up ProcessBuilder");
            ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);
            logger.trace("Starting Process");
            Process p = pb.start();

            if (!command.startsWith("sendudp")) {
                logger.trace("Starting Output Forwarders");
                StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), "Error",
                        plugin, exchangeID, dstRegion, dstAgent, dstPlugin);
                StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), "Output",
                        plugin, exchangeID, dstRegion, dstAgent, dstPlugin);

                errorGobbler.start();
                outputGobbler.start();
            }

            logger.trace("Waiting for process completion");
            int exitValue = p.waitFor();
            logger.trace("Process has completed");
            complete = true;
            running = false;
            if (!command.startsWith("sendudp")) {
                logger.trace("Sending exitValue log");
                Map<String, String> params = new HashMap<>();
                params.put("src_region", plugin.getRegion());
                params.put("src_agent", plugin.getAgent());
                params.put("src_plugin", plugin.getPluginID());
                params.put("dst_region", dstRegion);
                params.put("dst_agent", dstAgent);
                params.put("dst_plugin", dstPlugin);
                params.put("cmd", "execution_log");
                params.put("exchange", exchangeID);
                params.put("ts", Long.toString(new Date().getTime()));
                params.put("log", "[" + new Date() + "] Exit Code: " + Integer.toString(exitValue));
                plugin.sendMsgEvent(new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), params));

                logger.trace("Sending Exchange Deletion request");
                params = new HashMap<>();
                params.put("src_region", plugin.getRegion());
                params.put("src_agent", plugin.getAgent());
                params.put("src_plugin", plugin.getPluginID());
                params.put("inode_id", plugin.getConfig().getStringParam("inode_id"));
                params.put("resource_id", plugin.getConfig().getStringParam("resource_id"));
                params.put("dst_region", dstRegion);
                params.put("dst_agent", dstAgent);
                params.put("dst_plugin", dstPlugin);
                params.put("cmd", "delete_exchange");
                params.put("exchange", exchangeID);
                plugin.sendMsgEvent(new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), params));
            } else {
                logger.trace("Notify Caller That Exec has completed");
                Map<String, String> params = new HashMap<>();
                params.put("src_region", plugin.getRegion());
                params.put("src_agent", plugin.getAgent());
                params.put("src_plugin", plugin.getPluginID());
                params.put("dst_region", dstRegion);
                params.put("dst_agent", dstAgent);
                params.put("dst_plugin", dstPlugin);
                params.put("cmd", "exec_complete");
                params.put("exchange", exchangeID);
                plugin.sendMsgEvent(new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), params));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Boolean isRunning() {
        return this.running;
    }

    public void shutdown() {
        if (!complete) {
            logger.info("Killing process");
            try {
                if (command.toLowerCase().startsWith("kanon")) {
                    ProcessBuilder pb = new ProcessBuilder("sudo", "bash", "-c",
                            "kill -2 $(ps aux | grep '" +
                            command.substring(0, command.indexOf("'")) + "' | awk '{print $2}')");
                    Process p = pb.start();
                    try {
                        p.waitFor();
                        running = false;
                    } catch (InterruptedException e) {
                        // Todo: Maybe this should be pushed up the stack?
                    }
                } else {
                    ProcessBuilder pb = new ProcessBuilder("sudo", "bash", "-c",
                            "kill -2 $(ps aux | grep '[" +
                            exchangeID.charAt(0) + "]" + exchangeID.substring(1) + "' | awk '{print $2}')");
                    Process p = pb.start();
                    try {
                        p.waitFor();
                        running = false;
                    } catch (InterruptedException e) {
                        // Todo: Maybe this should be pushed up the stack?
                    }
                }
            } catch (IOException e) {
                logger.error("IOException in shutdown() : " + e.getMessage());
            }
        }
    }
}
