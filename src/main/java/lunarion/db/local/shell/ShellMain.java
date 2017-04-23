package lunarion.db.local.shell;

public class ShellMain {

	public static void main(String[] args) {
		CommandFactory factory = new CommandFactory();

		CommandStack.getInstance().cleanStack();
		CommandStack.getInstance()
				.setNextCommand(factory.getCommand(CMDEnumeration.entry_command.lunardb.toString(), null));

		while (CommandStack.getInstance().hasNext()) {
			CommandStack.getInstance().getNext().run();
		}

		System.out.println("bye");

		CommandStack.getInstance().cleanStack();

	}

}
