package edu.uky.irnc.executor;

import com.google.auto.service.AutoService;
import com.researchworx.cresco.library.plugin.core.CPlugin;
import oshi.SystemInfo;
import oshi.software.os.OperatingSystem;

@SuppressWarnings({"unused","WeakerAccess"})
@AutoService(CPlugin.class)
public class Plugin extends CPlugin {
    private Executor executor;

    public void start() {

        String runCommand = config.getStringParam("runCommand");
        String dstRegion = config.getStringParam("dstRegion");
        String dstAgent = config.getStringParam("dstAgent");
        String dstPlugin = config.getStringParam("dstPlugin");

        String exchangeID = runCommand.substring(runCommand.lastIndexOf(" ") + 1);
        this.executor = new Executor(this, runCommand, exchangeID, dstRegion, dstAgent, dstPlugin);
        setExec(executor);
    }

    @Override
    public void cleanUp() {
        executor.shutdown();
    }

    public static void main(String[] args) {
        System.out.println("This is not meant to be used outside of the Cresco framework.");
    }
}
