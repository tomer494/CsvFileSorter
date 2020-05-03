package com.ob1tech.CsvFileSorter;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import com.ob1tech.CsvFileSorter.CSVFileSorter.ARGS;
import com.ob1tech.CsvFileSorter.controllers.Controller;
import com.ob1tech.CsvFileSorter.controllers.ControllerBuilder;

/**
 * This is the gateway to csv file sorting tool.
 * This program aims to help you serialized huge unsorted
 * records by suppling the key index.
 *
 * Please run first time with option --help to get the full feature options
 * 
 * @author Madmon Tomer
 */
public class CSVFileSorter {
	
	public static enum ARGS{
		HELP("--help","Optional, See this help"),
		HAS_HEADER("-head","Indicate file has header record at first row"),
		HAS_NO_HEADER("-nohead","Indicate file has no header record at first row"),
		BUFFER_SIZE("-b<Size>","Optional, Indicate the max number of records to read at the same time."
				+ " Defualt is "+Controller.DEFAULT_BUFFER_SIZE+". "
				+ "Example: -b6 will indicate that 6 records will be read at each interval"),
		KEY_INDEX("-ki<index>","Optional, Indicate the key index in the csv record row."
				+ " Default is 0(first column). "
				+ "Example: -ki11 will indicate the key is in the 12th column"),
		KEY_TYPE("-t<type>","Inform of key value type for correct sorting. Mainly string or long."
				+ "Defualt is Long. Suports:"
				+ "string|double|long"
				+ "Example: -tstring");

		public static Map<String, ARGS> mapByValue = new HashMap<String, ARGS>(){
			private static final long serialVersionUID = -1037764536489571257L;

		{
			for(ARGS argType : ARGS.values()) {
				put(argType.value, argType);
			}
			
		}				
		};
		
		String value;
		String description;
		
		ARGS(String value, String description) {
			this.value = value;
			this.description = description;
		}
		
		public String getValue(){
			return this.value;
		}

		public static ARGS getByValue(String value) {
			return mapByValue.get(value);
		}
	}
	
	protected static byte skipHead = 0;
	protected static int argIndex = 0;
	protected static boolean argError = true;
	protected static boolean illegalArg = false;
	protected static boolean argHelp = false;
	protected static String filePath = "";
	protected static boolean fileError = false;
	protected static int keyIndex = 0;
	protected static int bufferSize = 0;
	protected static String keyType = null;
			
    	
    public static void main( String[] args )
    {
    	
    	CSVFileSorter csvFileSorter = new CSVFileSorter();
    	csvFileSorter.init(args);
    	
    	csvFileSorter.run(skipHead, filePath, keyIndex, bufferSize, keyType);
		
    	System.exit(0);
    }

	protected void init(String[] args) {
		//If no file or option is used then show help
    	if(args.length==0) {
    		args = new String[] {"-h"};
    	}
    	
    	
    	
		try {
			for(String arg : args) {
				if(argIndex == 0) {
					argError = false;
					filePath = arg;
					File file = new File( filePath );
				    if (file.exists() && file.isFile()) {
				      //OK!
				    }else {
				    	fileError = true;
				    	break;
				    }
					
					argIndex++;
					continue;
				}

				ARGS value = ARGS.getByValue(arg);
				if(value!=null) {
					switch(value) {
					case HELP: argHelp = true; break;
					case HAS_HEADER: skipHead = 1; break;
					case HAS_NO_HEADER: skipHead = -1; break;
					default:
						break;
					}
				}else {
					if(arg.startsWith(ARGS.KEY_INDEX.getValue().substring(0, 3))){
						keyIndex = Integer.valueOf(arg.substring(3));
					}; 
					if(arg.startsWith(ARGS.BUFFER_SIZE.getValue().substring(0, 2))){
						bufferSize = Integer.valueOf(arg.substring(2));
					}; 
					if(arg.startsWith(ARGS.KEY_TYPE.getValue().substring(0, 2))){
						keyType = arg.substring(2).toLowerCase();
					}; 
					
				}
				argIndex++;
			}
		} catch (Exception e) {
			argError = true;
			illegalArg = true;
			e.printStackTrace();
		}
		if(fileError) {
			System.out.println("Error: No file found at path "+filePath);
    		System.exit(0);
		}
    	if(argError) {
    		if(argIndex==0) {
    			System.out.println("No file specified!");
    		}else if(!illegalArg) {
    			System.out.println("Unknown option "+argIndex+1+", Run "+ARGS.HELP.getValue()+" for help");
    		}else {
    			System.out.println("Illegal option "+argIndex+1+", Run "+ARGS.HELP.getValue()+" for help");
    		}
    		System.exit(0);
    	}
    	if(argHelp) {
    		System.out.println(getHelp());
    		System.exit(0);
    	}
    	
    	//Head option was not specified
    	if(skipHead==0) {
    		boolean anwered;
    		Scanner in = null;
    		try{
    			in = new Scanner(System.in);
				do {
					anwered = true;
		    		System.out.print("Does the file has a header? Yes(y|1),No(n|0) >");
		    		  
		            String s = in.next(); 
		            switch(s) {
		            case "y":
		            case "1": skipHead = 1; break;
		            case "n":
		            case "0": skipHead = -1; break;
		            default: anwered = false;
		            }
	    		} while(!anwered);
    		} finally {
    			if(in!=null) {
    				in.close();
    			}
			}
    	}
	}

	protected void run(byte skipHead, String filePath, int keyIndex, int bufferSize, String keyType) {
		
    	ControllerBuilder builder = new ControllerBuilder(filePath);
    	
    	builder.withSkipHeader(skipHead>0);
    	
		builder.withKeyIndex(keyIndex );
		builder.withBatchSize( bufferSize );
		builder.withKeyType( keyType );
    	
		Controller<?> controller = builder.build();
    	controller.execute();
    	
	}

	protected static String getHelp() {
		String output = "java -jar csvFileSort.jar <File-path> options?\n"
				+ "options:\n";
		String optionStr = "\t%s: %s\n";
		for(ARGS argType : ARGS.values()) {
			output += String.format(optionStr, argType.value, argType.description);
		}
		return output;
	}
}
