package lunarion.db.local.shell.command;

import java.io.BufferedReader; 
import java.io.IOException;
import java.io.InputStreamReader;

import LCG.DB.API.LunarDB;
import LCG.FSystem.Def.DBFSProperties;
import LCG.Utility.StrLegitimate;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.db.local.shell.CommandFactory;
import lunarion.db.local.shell.ShellCommand;
import lunarion.db.local.shell.CMDEnumeration.entry_command;

public class CreateDB extends ShellCommand {
	public static final String command_name = CMDEnumeration.command.createDB.toString();

	private String creation_conf_file = null;
	private String db_target_path = null;
	public CreateDB(CommandFactory cf, String creation_conf, String target_dir) {
		super(cf);
		creation_conf_file = creation_conf;
		db_target_path = target_dir;
	}

	@Override
	public boolean comfirmed(String input) {
		if (input.equalsIgnoreCase("y") || input.equalsIgnoreCase("n"))
			return true;
		else {
			System.out.println("please input Y(create) or N(quit)");

			return false;
		}
	}

	@Override
	public String getInput() {
		try {
			System.out.println("input Y(create) or N(quit)");
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			String input = in.readLine();
			return input;
		} catch (Exception x) {
			x.printStackTrace();
		}
		return null;
	}

	public ShellCommand getNextCommand(CommandFactory factory, String input) {
		if (input.equalsIgnoreCase(entry_command.quit.toString()))
			return null;
		else if (!input.equalsIgnoreCase("y") && !input.equalsIgnoreCase("n"))
			return this;
		else
			return factory.getCommand(CMDEnumeration.entry_command.lunardb.toString(), null);
	}

	@Override
	public String getCommandName() {
		return command_name;
	}

	@Override
	public void executeCommand(String input) {
		if (input.equalsIgnoreCase("y")) {
			if (this.creation_conf_file == null) {
				System.err.println("please input a correct configuration file for creating a lunarDB");
			}
			LunarDB.getInstance().createDB(this.db_target_path, this.creation_conf_file);
			try {
				LunarDB.getInstance().closeDB();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else
			return;
	}

}
