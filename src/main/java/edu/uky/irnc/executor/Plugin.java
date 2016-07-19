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
        String dstPlugin = config.getStringParam("dstPlugin");
        exchangeID = runCommand.substring(runCommand.lastIndexOf(" ") + 1);
        executeCommand(runCommand, dstPlugin);
    }

    @Override
    public void cleanUp() {
        runner.shutdown();
    }

    private void executeCommand(String command, String dstPlugin) {
        runner = new Runner(this, command, dstPlugin);
        new Thread(runner).start();
    }

    private static class Runner implements Runnable {
        private static Set<String> executables = new HashSet<>(Arrays.asList("netflow", "packet_trace",
                "packet_validation", "sendudp"));
        private Plugin plugin;
        private CLogger logger;
        private String command;
        private String dstPlugin;
        private boolean complete = false;

        Runner(Plugin plugin, String command, String dstPlugin) {
            this.plugin = plugin;
            this.command = command;
            this.dstPlugin = dstPlugin;
            logger = new CLogger(Runner.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(),
                    plugin.getPluginID(), CLogger.Level.Trace);
        }

        @Override
        public void run() {
            try {
                boolean canRun = false;
                for (String executable : executables)
                    if (command.startsWith(executable))
                        canRun = true;
                if (!canRun) return;
                ProcessBuilder pb = new ProcessBuilder("sudo","bash","-c", command);
                final Process p = pb.start();

                StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), plugin, dstPlugin);
                StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), plugin, dstPlugin);

                errorGobbler.start();
                outputGobbler.start();

                int exitValue = p.waitFor();

                Map<String, String> params = new HashMap<>();
                params.put("src_region", plugin.getRegion());
                params.put("src_agent", plugin.getAgent());
                params.put("src_plugin", plugin.getPluginID());
                params.put("dst_region", plugin.getRegion());
                params.put("dst_agent", plugin.getAgent());
                params.put("dst_plugin", dstPlugin);
                params.put("cmd", "execution_log");
                params.put("exchange", exchangeID);
                params.put("log", "[" + new Date() + "] " + Integer.toString(exitValue));
                plugin.sendMsgEvent(new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), params));

                complete = true;
                params = new HashMap<>();
                params.put("src_region", plugin.getRegion());
                params.put("src_agent", plugin.getAgent());
                params.put("src_plugin", plugin.getPluginID());
                params.put("dst_region", plugin.getRegion());
                params.put("dst_agent", plugin.getAgent());
                params.put("dst_plugin", dstPlugin);
                params.put("cmd", "delete_exchange");
                params.put("exchange", exchangeID);
                plugin.sendMsgEvent(new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                        plugin.getPluginID(), params));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private static class StreamGobbler extends Thread {
            private InputStream is;
            private Plugin plugin;
            private CLogger logger;
            private String dstPlugin;

            StreamGobbler(InputStream is, Plugin plugin, String dstPlugin) {
                this.is = is;
                this.plugin = plugin;
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
                        params.put("dst_region", plugin.getRegion());
                        params.put("dst_agent", plugin.getAgent());
                        params.put("dst_plugin", dstPlugin);
                        params.put("cmd", "execution_log");
                        params.put("exchange", exchangeID);
                        params.put("log", "[" + new Date() + "] " + line);
                        plugin.sendMsgEvent(new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                                plugin.getPluginID(), params));
                    }
                    br.close();
                } catch (IOException e) {
                    logger.error("run() : {}", e.getMessage());
                }
            }
        }

        void shutdown() {
            if (!complete) {
                logger.info("Killing process");
                try {
                    ProcessBuilder pb = new ProcessBuilder("sudo", "bash", "-c", "kill -2 $(ps aux | grep '[" +
                            exchangeID.charAt(0) + "]" + exchangeID.substring(1) + "' | awk '{print $2}')");
                    pb.start();
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
