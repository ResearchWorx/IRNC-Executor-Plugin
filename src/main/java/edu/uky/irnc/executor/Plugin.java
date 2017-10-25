package edu.uky.irnc.executor;

import com.google.auto.service.AutoService;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import com.researchworx.cresco.library.utilities.CLogger;

import java.io.*;
import java.util.*;

@SuppressWarnings({"unused","WeakerAccess"})
@AutoService(CPlugin.class)
public class Plugin extends CPlugin {
    private static String exchangeID;
    private static Runner runner;

    public void start() {
        String runCommand = config.getStringParam("runCommand");
        //n0Params.put("dstRegion", plugin.getRegion());
        //n0Params.put("dstAgent", plugin.getAgent());
        //n0Params.put("dstPlugin", plugin.getPluginID());

        String dstRegion = config.getStringParam("dstRegion");
        String dstAgent = config.getStringParam("dstAgent");
        String dstPlugin = config.getStringParam("dstPlugin");

        String requiresSudo = config.getStringParam("requiresSudo", "true");
        logger.info("sudo = {}", requiresSudo);
        exchangeID = runCommand.substring(runCommand.lastIndexOf(" ") + 1);
        executeCommand(runCommand, dstRegion, dstAgent, dstPlugin, requiresSudo);
    }

    @Override
    public void cleanUp() {
        runner.shutdown();
    }

    private void executeCommand(String command, String dstRegion, String dstAgent, String dstPlugin, String requiresSudo) {
        runner = new Runner(this, command, dstRegion, dstAgent, dstPlugin, requiresSudo);
        new Thread(runner).start();
    }

    private static class Runner implements Runnable {
        private static Set<String> executables = new HashSet<>(Arrays.asList("netflow", "packet_trace",
                "packet_validation", "sendudp", "kanon", "perfSONAR_Throughput", "amis_argus", "ls", "/home/yan/AMIS/amis"));
        private Plugin plugin;
        private CLogger logger;
        private String command;
        private String dstRegion;
        private String dstAgent;
        private String dstPlugin;
        private String requiresSudo;
        private Process p;
        private boolean complete = false;

        Runner(Plugin plugin, String command, String dstRegion, String dstAgent, String dstPlugin, String requiresSudo) {
            this.plugin = plugin;
            this.command = command;
            this.dstRegion = dstRegion;
            this.dstAgent = dstAgent;
            this.dstPlugin = dstPlugin;
            this.requiresSudo = requiresSudo;
            logger = new CLogger(Runner.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(),
                    plugin.getPluginID(), CLogger.Level.Trace);
        }

        @Override
        public void run() {
            try {
                logger.info("Running Runner");
                //logger.info("Command: [" + command + "]");
                //logger.info("command={}", command);
                boolean canRun = false;
                logger.trace("Checking to see if eligable for running");
                for (String executable : executables)
                    if (command.startsWith(executable))
                        canRun = true;
                logger.debug("canRun = {}", canRun);
                if (!canRun) return;
                logger.trace("Setting up ProcessBuilder");
                ProcessBuilder pb;
                if (requiresSudo.equals("true"))
                    pb = new ProcessBuilder("sudo", "bash", "-c", command);
                else
                    pb = new ProcessBuilder("/bin/sh", "-c", command);
                logger.trace("Starting Process");
                p = pb.start();

                if (!command.startsWith("sendudp")) {
                    logger.trace("Starting Output Forwarders");
                    StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), plugin, dstRegion, dstAgent, dstPlugin);
                    StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), plugin, dstRegion, dstAgent, dstPlugin);

                    errorGobbler.start();
                    outputGobbler.start();
                }

                logger.trace("Waiting for process completion");
                int exitValue = p.waitFor();
                logger.trace("Process has completed");
                complete = true;
                if (!command.startsWith("sendudp")) {
                    logger.trace("Sending exitValue log");
                    Map<String, String> params = new HashMap<>();
                    params.put("src_region", plugin.getRegion());
                    params.put("src_agent", plugin.getAgent());
                    params.put("src_plugin", plugin.getPluginID());
                    //params.put("dst_region", plugin.getRegion());
                    //params.put("dst_agent", plugin.getAgent());
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
                    //params.put("dst_region", plugin.getRegion());
                    //params.put("dst_agent", plugin.getAgent());
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
                    params = new HashMap<>();
                    params.put("src_region", plugin.getRegion());
                    params.put("src_agent", plugin.getAgent());
                    params.put("src_plugin", plugin.getPluginID());
                    //params.put("dst_region", plugin.getRegion());
                    //params.put("dst_agent", plugin.getAgent());
                    params.put("dst_region", dstRegion);
                    params.put("dst_agent", dstAgent);
                    params.put("dst_plugin", dstPlugin);
                    params.put("cmd", "exec_complete");
                    params.put("exchange", exchangeID);
                    plugin.sendMsgEvent(new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                            plugin.getPluginID(), params));
                    /*
                    Thread.sleep(5000);
                    logger.trace("Sending Plugin Removal request");
                    Map<String, String> params = new HashMap<>();
                    params.put("src_region", plugin.getRegion());
                    params.put("src_agent", plugin.getAgent());
                    params.put("src_plugin", plugin.getPluginID());
                    params.put("dst_region", plugin.getRegion());
                    params.put("dst_agent", plugin.getAgent());
                    params.put("configtype", "pluginremove");
                    params.put("plugin", plugin.getPluginID());
                    plugin.sendMsgEvent(new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(),
                            plugin.getPluginID(), params));
                            */
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private static class StreamGobbler extends Thread {
            private InputStream is;
            private Plugin plugin;
            private CLogger logger;
            private String dstRegion;
            private String dstAgent;
            private String dstPlugin;

            StreamGobbler(InputStream is, Plugin plugin, String dstRegion, String dstAgent, String dstPlugin) {
                this.is = is;
                this.plugin = plugin;
                this.dstRegion = dstRegion;
                this.dstAgent = dstAgent;
                this.dstPlugin = dstPlugin;

                logger = new CLogger(StreamGobbler.class, plugin.getMsgOutQueue(), plugin.getRegion(),
                        plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);
            }

            @Override
            public void run() {
                try {
                    InputStreamReader isr = new InputStreamReader(is);
                    BufferedReader br = new BufferedReader(isr);
                    String line;
                    while ( ( line = br.readLine() ) != null ) {
                        logger.debug("Output: {}", line);
                        Map<String, String> params = new HashMap<>();
                        params.put("src_region", plugin.getRegion());
                        params.put("src_agent", plugin.getAgent());
                        params.put("src_plugin", plugin.getPluginID());
                        //params.put("dst_region", plugin.getRegion());
                        //params.put("dst_agent", plugin.getAgent());
                        params.put("dst_region", dstRegion);
                        params.put("dst_agent", dstAgent);
                        params.put("dst_plugin", dstPlugin);
                        params.put("ts", Long.toString(new Date().getTime()));
                        params.put("cmd", "execution_log");
                        params.put("exchange", exchangeID);
                        params.put("log", "[" + new Date() + "] " + line);
                        plugin.sendMsgEvent(new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                                plugin.getPluginID(), params));
                        Thread.sleep(50);
                    }
                    br.close();
                } catch (IOException e) {
                    logger.error("run() : {}", e.getMessage());
                } catch (InterruptedException e) {
                    logger.error("run() : Interrupted : {}", e.getMessage());
                }
            }
        }

        void shutdown() {
            if (!complete) {
                logger.info("Killing process");
                try {
                    if (command.toLowerCase().startsWith("kanon")) {
                        ProcessBuilder pb = new ProcessBuilder("sudo", "bash", "-c", "kill -2 $(ps aux | grep '" +
                                command.substring(0, command.indexOf("'")) + "' | awk '{print $2}')");
                        pb.start();
                    } else {
                        ProcessBuilder pb = new ProcessBuilder("sudo", "bash", "-c", "kill -2 $(ps aux | grep '[" +
                                exchangeID.charAt(0) + "]" + exchangeID.substring(1) + "' | awk '{print $2}')");
                        pb.start();
                    }
                } catch (IOException e) {
                    logger.error("IOException in shutdown() : " + e.getMessage());
                }
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("This is not meant to be used outside of the Cresco framework.");
    }
}
