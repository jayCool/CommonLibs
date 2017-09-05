/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package db.structs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Zhang Jiangwei
 */
public class ReadFullTable implements Runnable {

    private String tableName;
    private String delimiter;
    private boolean ignoreFirstLine;
    private String inputDir;
    private int fkSize;
    private Table[] tables;
    private HashMap<String, Integer> tableSize;
    private int tableNumber;
    private int leading;
    private String loadingOption;
    private  String suffix;
    ReadFullTable(String tableName, boolean ignoreFirstLine, int tableNumber, HashMap<String, Integer> tableSizeMap,
            int leading, String delimiter, String loadingOption, int fkSize, String inputDir, String suffix, Table[] tables) {
        this.tableName = tableName;
        this.ignoreFirstLine = ignoreFirstLine;
        this.tableNumber = tableNumber;
        this.tableSize = tableSizeMap;
        this.leading = leading;
        this.delimiter = delimiter;
        this.loadingOption = loadingOption;
        this.fkSize = fkSize;
        this.inputDir = inputDir;
        this.suffix = suffix;
        this.tables = tables;
    }



    @Override
    public void run() {
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(inputDir + "/" + tableName + suffix);
            BufferedReader scanner = new BufferedReader(new InputStreamReader(inputStream), 5000000);
            if (this.ignoreFirstLine) {
                scanner.readLine().trim().split(delimiter);
            }

            Table tableObject = new Table();
            tables[this.tableNumber] = tableObject;
            tables[this.tableNumber].tableName = this.tableName;
            tables[this.tableNumber].fkSize = this.fkSize;
          
            if (loadingOption.equals(Options.loadFK)) {
                loadTableFKOption(tableObject, scanner);
            } else if (loadingOption.equals(Options.loadFull)){
                loadTableFullOption(tableObject, scanner);
            }

            scanner.close();
            scanner = null;
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ReadFullTable.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ReadFullTable.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                inputStream.close();
            } catch (IOException ex) {
                Logger.getLogger(ReadFullTable.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
     private void loadTableFullOption(Table tableObject, BufferedReader scanner) {
        try {
            String input = scanner.readLine().trim();
            StringTokenizer tokenizer;
            int attributeLength = input.trim().split(delimiter).length;
            int[][] fks = new int[this.tableSize.get(tableName)][fkSize];
            int[][] fullNonKeys = new int[this.tableSize.get(tableName)][attributeLength-1-fkSize];
            int[] maxNum = new int[this.tableSize.get(tableName)];
            while (input != null) {
                tokenizer = new StringTokenizer(input.trim());
                int pkID = Integer.parseInt(tokenizer.nextToken()) - this.leading;
                for (int fkIndex = 1; fkIndex < attributeLength && fkIndex < this.fkSize + 1; fkIndex++) {
                    fks[pkID][fkIndex - 1] = Integer.parseInt(tokenizer.nextToken()) - this.leading;
                }
                
                
                for (int attributeIndex = this.fkSize + 1; attributeIndex < attributeLength; attributeIndex++) {
                    int nonKeyID = Integer.parseInt(tokenizer.nextToken()) - this.leading;
                    fullNonKeys[pkID][attributeIndex-this.fkSize-1] = nonKeyID;
                    maxNum[attributeIndex - fkSize - 1] = Math.max(maxNum[attributeIndex - fkSize - 1], nonKeyID);
                }

                input = scanner.readLine();

            }
            tableObject.fks = fks;
            tableObject.fullNonKeys = fullNonKeys;
            tableObject.nonKeyMaxNum = maxNum;
            
        } catch (IOException ex) {
            Logger.getLogger(ReadFullTable.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void loadTableFKOption(Table tableObject, BufferedReader scanner) {
        try {
            String input = scanner.readLine().trim();
            StringTokenizer tokenizer;
            int attributeLength = input.trim().split(delimiter).length;
            int[][] fks = new int[this.tableSize.get(tableName)][fkSize];
            String[] nonKeys = new String[this.tableSize.get(tableName)];
            while (input != null) {
                tokenizer = new StringTokenizer(input.trim());
                int pkID = Integer.parseInt(tokenizer.nextToken()) - this.leading;
                for (int fkIndex = 1; fkIndex < attributeLength && fkIndex < this.fkSize + 1; fkIndex++) {
                    fks[pkID][fkIndex - 1] = Integer.parseInt(tokenizer.nextToken()) - this.leading;
                }
                String nonkey = "";
                
                for (int attributeIndex = this.fkSize + 1; attributeIndex < attributeLength; attributeIndex++) {
                    if (tokenizer.hasMoreTokens()) {
                        nonkey += "\t" + tokenizer.nextToken();
                    } else {
                        nonkey += "\t" + "null";
                    }
                }
                if (!nonkey.isEmpty()){
                nonKeys[pkID] = nonkey.trim();
                }
                input = scanner.readLine();

            }
            
            tableObject.fks = fks;
            tableObject.nonKeys = nonKeys;
        } catch (IOException ex) {
            Logger.getLogger(ReadFullTable.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
