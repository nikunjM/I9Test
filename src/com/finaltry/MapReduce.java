package com.finaltry;
import javax.swing.JFileChooser;
import javax.swing.filechooser.FileNameExtensionFilter;

import org.apache.commons.io.FilenameUtils;


public class MapReduce {

	public static void main(String[] args) throws Exception {
		String fileSlectore=fileselectore();
		String ext = FilenameUtils.getExtension(fileSlectore);
		System.out.println("asd"+ext);
		if(fileSlectore==null)
		{
			System.out.println("Please select file for input");
		}else{
			Main lmain=new Main();
			lmain.main(fileSlectore);
		}
	}
	static String fileselectore(){
		  JFileChooser chooser = new JFileChooser();
		    chooser.setCurrentDirectory(new java.io.File("."));
		    chooser.setDialogTitle("choosertitle");
		    chooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
		    chooser.setAcceptAllFileFilterUsed(false);
		    
		    
		    FileNameExtensionFilter filter = new FileNameExtensionFilter("TEXT FILES", "txt", "text");
		    chooser.setFileFilter(filter);
		    
		    if (chooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
		      System.out.println("getCurrentDirectory(): " + chooser.getCurrentDirectory()+"---------"+chooser.getSelectedFile().toString());
		     return chooser.getSelectedFile().toString();
		    } else {
		      System.out.println("No Selection");
		      return null;
		    }
		  }

}
	