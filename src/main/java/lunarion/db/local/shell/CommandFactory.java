package lunarion.db.local.shell;

import lunarion.db.local.shell.command.CreateDB;
import lunarion.db.local.shell.command.LunarDBShell;

public class CommandFactory {
	public ShellCommand getCommand(String command, String[] param) {
		if (command.equals(CMDEnumeration.entry_command.lunardb.toString())) {
			return new LunarDBShell(this, null);
		} else if (command.equals(CMDEnumeration.command.createDB.toString())) {
			return new CreateDB(this, param[0],  param[1]);
		} else if (command.equals(CMDEnumeration.command.insert.toString())) {
			return null;
		} else if (command.equals(CMDEnumeration.command.ftQuery.toString())) {
			return null;
		} else if (command.equals(CMDEnumeration.command.rebuild.toString())) {
			return null;
		} else if (command.equals(CMDEnumeration.command.createDB.toString())) {
			return null;
		} else if (command.equals(CMDEnumeration.command.createDB.toString())) {
			return null;
		} else if (command.equals(CMDEnumeration.command.createDB.toString())) {
			return null;
		} else if (command.equals(CMDEnumeration.command.createDB.toString())) {
			return null;
		} else if (command.equals(CMDEnumeration.command.createDB.toString())) {
			return null;
		}
		return null;
	}
}
