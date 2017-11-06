package edu.uky.irnc.executor;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.plugin.core.CExecutor;
import com.researchworx.cresco.library.utilities.CLogger;
import edu.uky.irnc.executor.helpers.Runner;

import java.util.HashMap;
import java.util.Map;

public class Executor extends CExecutor {
    private final CLogger logger;
    private final Plugin plugin;
    private final String command;
    private final String exchangeID;
    private final String dstRegion;
    private final String dstAgent;
    private final String dstPlugin;

    private Runner runner;

    Executor(Plugin plugin, String command, String exchangeID, String dstRegion, String dstAgent, String dstPlugin) {
        super(plugin);
        this.plugin = plugin;
        this.logger = new CLogger(Executor.class, plugin.getMsgOutQueue(), plugin.getRegion(),
                plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);
        this.command = command;
        this.exchangeID = exchangeID;
        this.dstRegion = dstRegion;
        this.dstAgent = dstAgent;
        this.dstPlugin = dstPlugin;
    }

    @Override
    public MsgEvent processExec(MsgEvent msg) {
        logger.debug("Processing EXEC message: {}", msg.getParams());
        /*
        Map<String, String> params = new HashMap<>();
        params.put("src_region", plugin.getRegion());
        params.put("src_agent", plugin.getAgent());
        params.put("src_plugin", plugin.getPluginID());
        params.put("dst_region", dstRegion);
        params.put("dst_agent", dstAgent);
        params.put("dst_plugin", dstPlugin);
        */
        msg.setParam("error", Boolean.toString(false));
        switch (msg.getParam("cmd")) {
            case "run_process":
                logger.trace("{} cmd received", msg.getParam("cmd"));
                if (runner != null) {
                    if(runner.isRunning()) {
                        logger.error("Trying to run, but runner != null, STOP first.");
                        msg.setParam("error", Boolean.toString(true));
                        msg.setParam("error_msg", "Process is already running");
                        return msg;
                    }
                    runner = null;
                }
                runner = new Runner(plugin, command, exchangeID, dstRegion, dstAgent, dstPlugin);
                new Thread(runner).start();
                //todo do some status here
                msg.setParam("status", Boolean.toString(true));
                //logger.info("Returning Message");
                //MsgEvent returnMsg = new MsgEvent(MsgEvent.Type.EXEC, plugin.getRegion(), plugin.getAgent(),
                //        plugin.getPluginID(), params);
                //logger.info("Returning Message sent : " + returnMsg.getParams());

                return msg;
            case "status_process":
                logger.trace("{} cmd received", msg.getParam("cmd"));
                if (runner == null || !runner.isRunning()) {
                    msg.setParam("status_runner", Boolean.toString(runner == null));
                    if (runner == null)
                        msg.setParam("status_process", Boolean.toString(false));
                    else
                        msg.setParam("status_process", Boolean.toString(runner.isRunning()));
                } else
                    msg.setParam("status", Boolean.toString(true));
                return msg;
            case "end_process":
                logger.trace("{} cmd received", msg.getParam("cmd"));
                if (runner == null || !runner.isRunning()) {
                    msg.setParam("error", Boolean.toString(true));
                    if (runner == null)
                        msg.setParam("error_msg", "Process is not currently running");
                    else
                        msg.setParam("error_msg", "Process could not be run");
                    return msg;
                }
                runner.shutdown();
                runner = null;

                msg.setParam("status", Boolean.toString(true));

                return msg;
            default:
                logger.error("Unknown cmd: {}", msg.getParam("cmd"));
                msg.setParam("error", Boolean.toString(true));
                msg.setParam("error_msg", "Unknown cmd  [" + msg.getParam("cmd") + "]");
                return msg;
        }

    }

    void shutdown() {
        logger.debug("Call to Shutdown()");
        runner.shutdown();
        runner = null;
    }
}
