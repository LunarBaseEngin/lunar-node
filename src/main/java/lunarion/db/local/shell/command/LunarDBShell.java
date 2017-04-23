package lunarion.db.local.shell.command;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import lunarion.db.local.shell.CMDEnumeration;
import lunarion.db.local.shell.CommandFactory;
import lunarion.db.local.shell.ShellCommand;
import lunarion.db.local.shell.CMDEnumeration.entry_command; 

public class LunarDBShell extends ShellCommand {
	public static final String command_name = CMDEnumeration.entry_command.lunardb.toString();

	public LunarDBShell(CommandFactory cf, String param) {
		super(cf);
	}

	@Override
	public boolean comfirmed(String input) {
		if (validate(input) != null)
			return true;
		else {
			System.out.println("please input correct command to proceed");
			return false;
		}
	}

	@Override
	public String getInput() {
		try {
			System.out.println("LunarDB Console:> ");
			System.out.println();
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			String input = in.readLine();
			return input;
		} catch (Exception x) {
			x.printStackTrace();
		}
		return null;
	}

	private String validate(String input) {
		if (input.startsWith(CMDEnumeration.command.createDB.toString() + " ") || input.startsWith(CMDEnumeration.command.insert.toString() + " "))
			return input;
		else
			return null;
	}

	public ShellCommand getNextCommand(CommandFactory factory, String input) {
		if (input.equalsIgnoreCase(CMDEnumeration.entry_command.quit.toString()))
			return null;
		String _input_command = validate(input);
		if (_input_command != null) {
			String[] command_param = _input_command.trim().split(" ");
			if (command_param.length < 2) {
				System.err.println("command error, parameter is missing");
				return factory.getCommand(CMDEnumeration.entry_command.lunardb.toString(), null);
			}
			String[] params = new String[command_param.length-1];
			for(int i=1;i<command_param.length;i++)
				params[i-1] = command_param[i];
			
			return factory.getCommand(command_param[0], params);
		} else
			return factory.getCommand(CMDEnumeration.entry_command.lunardb.toString(), null);
	}

	@Override
	public String getCommandName() {
		return command_name;
	}

	@Override
	public void executeCommand(String input) {

	}

}
