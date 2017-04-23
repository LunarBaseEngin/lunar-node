package LCG.DB.Local.Shell;

import lunarion.db.local.shell.CMDEnumeration;
import lunarion.db.local.shell.CommandFactory;
import lunarion.db.local.shell.CommandStack;

public class TestShell {

	public static void main(String[] args) {
		CommandFactory factory = new CommandFactory();
		
		CommandStack.getInstance().cleanStack(); 
		CommandStack.getInstance().setNextCommand(
                factory.getCommand(CMDEnumeration.entry_command.lunardb.toString(), null));
        
        while (CommandStack.getInstance().hasNext())
        {
        	CommandStack.getInstance().getNext().run();
        }
        
        System.out.println("bye");
        
        CommandStack.getInstance().cleanStack(); 

	}

}
