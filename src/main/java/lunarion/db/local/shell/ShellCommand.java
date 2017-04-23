package lunarion.db.local.shell;

 
import lunarion.db.local.shell.CMDEnumeration.entry_command;

public abstract class ShellCommand {

	private CommandFactory c_factory;

	public ShellCommand(CommandFactory cf) {
		c_factory = cf;
	}

	public abstract String getInput();

	public abstract boolean comfirmed(String input);

	public abstract String getCommandName();

	public abstract ShellCommand getNextCommand(CommandFactory factory, String input);

	public abstract void executeCommand(String input);

	public final void run() {

		String input = getInput();

		if (input.startsWith(entry_command.quit.toString())) {
			CommandStack.getInstance().setNextCommand(null);
			return;
		} else if (input.equalsIgnoreCase(entry_command.lunardb.toString())) {
			CommandStack.getInstance().setNextCommand(getNextCommand(c_factory, input));
		} else {
			if (comfirmed(input))
				executeCommand(input);

			CommandStack.getInstance().setNextCommand(getNextCommand(c_factory, input));

		}
	}
}
