package lunarion.db.local.shell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandStack {
	private List<ShellCommand> command_stack = new ArrayList<ShellCommand>();

	private static CommandStack command_stack_instance = new CommandStack();

	private CommandStack() {
	}

	public static CommandStack getInstance() {
		return command_stack_instance;
	}

	public void setNextCommand(ShellCommand c) {
		command_stack.add(c);
	}

	public void popLastCommand() {
		int len = command_stack.size();
		command_stack.remove(len - 1);
	}

	public ShellCommand getNext() {
		int len = command_stack.size();
		return command_stack.get(len - 1);
	}

	public boolean hasNext() {
		int len = command_stack.size();
		if (command_stack.get(len - 1) == null)
			return false;
		else
			return true;
	}

	public void cleanStack() {
		command_stack.clear();
	}

	public ShellCommand gotoCommand(int index) {
		if (index < command_stack.size()) {
			command_stack = command_stack.subList(0, index);
		}
		return getNext();
	}

	public int getStackSize() {
		return command_stack.size();
	}
}
