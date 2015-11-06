//*****************************************************
// Created by Adam Wanninger, wanninaj@miamioh.edu
// Downloaded from github.com/adam-wanninger
// GNU GENERAL PUBLIC LICENSE, Version 3, 29 June 2007
//*****************************************************

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.sql.Timestamp;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Run {
	
	public static void main(String args[]) throws IOException {
		int count = 0;
		java.util.Date date= new java.util.Date();
		Timestamp currentTime = new Timestamp(date.getTime());
		String postFix = new SimpleDateFormat("MMddyy_HHmmssSSS").format(currentTime);
		PrintWriter logWriter = new PrintWriter("RunLog_" + postFix + ".log", "UTF-8");
		logWriter.println("*****************************************LOG FILE FOR RUN.CLASS*****************************************");
		
			for (String filename: args) {
				//read in our MapReduce commands
				logWriter.println("* reading commands from " + filename);
				String[] commands = readCommands(filename);
				//execute commands
				logWriter.println("* running commands within " + filename);
				executeCommands(commands);
				logWriter.println("* success for all commands within " + filename);
				count++;
			}
		logWriter.println("* successfully processed " + count + " files");
		logWriter.close();
	}
	
	public static String[] readCommands(String filename) throws IOException {
		FileReader fileReader = new FileReader(filename);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		List<String> commands = new ArrayList<String>();
		String command = null;
		
		while ((command = bufferedReader.readLine()) != null) {
			commands.add(command);
		}		
		bufferedReader.close();
		return commands.toArray(new String[commands.size()]);
	}
	
	public static void executeCommands(String [] commands) {
		for (String MapReduceCommand: commands) {
			String s;
			Process p;
			
			try {
				p = Runtime.getRuntime().exec(MapReduceCommand);
				BufferedReader br = new BufferedReader(
				new InputStreamReader(p.getInputStream()));
				while ((s = br.readLine()) != null)
				System.out.println("line: " + s);
				p.waitFor();
				System.out.println ("exit: " + p.exitValue());
				p.destroy();
			} catch (InterruptedException i) { 
				System.out.println("if the current thread is interrupted by another thread while it is waiting, then the wait is ended and an InterruptedException is thrown");
				System.out.println("http://docs.oracle.com/javase/7/docs/api/java/lang/Process.html#waitFor()");
				System.exit(-1);
			} catch (SecurityException c) {
				System.out.println("SecurityException - thrown if security manager exists and its checkExec method doesn't allow creation of the subprocess");
				System.out.println("See:  http://docs.oracle.com/javase/7/docs/api/java/lang/Runtime.html#exec(java.lang.String)");
				System.exit(1);
			} catch (IOException o) {
				System.out.println("IOException - thrown if an I/O error occurs");
				System.out.println("See: http://docs.oracle.com/javase/7/docs/api/java/lang/Runtime.html#exec(java.lang.String)");
				System.exit(2);
			} catch (NullPointerException n) {
				System.out.println("NullPointerException - thrown if command is null");
				System.out.println("See: http://docs.oracle.com/javase/7/docs/api/java/lang/Runtime.html#exec(java.lang.String)");
				System.exit(3);
			} catch (IllegalArgumentException l) {
				System.out.println("IllegalArgumentException - thrown if command is empty");
				System.out.println("See: http://docs.oracle.com/javase/7/docs/api/java/lang/Runtime.html#exec(java.lang.String)");
				System.exit(4);
			}
			
		}
	}
	
}
