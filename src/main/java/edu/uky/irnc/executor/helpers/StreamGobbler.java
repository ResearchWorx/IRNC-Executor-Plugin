package edu.uky.irnc.executor.helpers;

import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import edu.uky.irnc.executor.Plugin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class StreamGobbler extends Thread {
    private InputStream is;
    private String label;
    private Plugin plugin;
    private CLogger logger;
    private String exchangeID;
    private String dstRegion;
    private String dstAgent;
    private String dstPlugin;

    StreamGobbler(InputStream is, String label, Plugin plugin, String exchangeID, String dstRegion,
                  String dstAgent, String dstPlugin) {
        logger = new CLogger(StreamGobbler.class, plugin.getMsgOutQueue(), plugin.getRegion(),
                plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);
        this.is = is;
        this.label = label;
        this.plugin = plugin;
        this.exchangeID = exchangeID;
        this.dstRegion = dstRegion;
        this.dstAgent = dstAgent;
        this.dstPlugin = dstPlugin;
    }

    @Override
    public void run() {
        logger.trace("StreamGobbler [{}] started for exchange [{}]", label, exchangeID);
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
