package net.neoremind.sshxcute.junit;

import junit.framework.TestCase;
import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.IOptionName;
import net.neoremind.sshxcute.core.Result;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;

/**
 * 
 * Test case number: FVT_CMD_002
 * 
 * Objective: Reset error msg keyword
 *  
 * Procedure: 1) Connect to Linux server
 * 			  2) Exec command "abcd"
 *   
 * Expected Results:
 *            2) Return correct response.
 * 
 * @author neo
 *
 */
public class FVT_CMD_005 extends TestCase{

	public void setUp() throws Exception {
		super.setUp();
	}

	public void testFVT_CMD_005()
	{
		SSHExec ssh = null;
		try {
			SSHExec.setOption(IOptionName.HALT_ON_FAILURE, false);
			ConnBean cb = new ConnBean("rfidic-1.svl.ibm.com", "tsadmin","u7i8o9p0");
			ssh = SSHExec.getInstance(cb);
			String[] reset_keyword = { "123" };
			CustomTask ct1 = new ExecCommand("exit 0");
			ct1.resetErrSysoutKeyword(reset_keyword);
			ssh.connect();
			Result r1 = ssh.exec(ct1);
			assertEquals(r1.rc, 0);
			System.out.println("Return code: " + r1.rc);
			System.out.println("sysout: " + r1.sysout);
			System.out.println("error msg: " + r1.error_msg);
		} catch (TaskExecFailException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			ssh.disconnect();	
		}
		
	}
	
	public void tearDown() throws Exception {
		super.tearDown();
	}
}
