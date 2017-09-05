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
 * @author workshop
 */
public class ParaReader implements Runnable {

    String table;
    String delim;
    boolean ignoreFirst;
    String filePath;
    Integer fkSize;
    Table[] tables;
    HashMap<String, Integer> tableSize;
    int tableNum;
    int leading;

    ParaReader(String table) {
        this.table = table;
    }

    @Override
    public void run() {
        FileInputStream input1 = null;
        try {
            input1 = new FileInputStream(filePath + "/" + table + ".txt");
            BufferedReader scanner = new BufferedReader(new InputStreamReader(input1), 5000000);
            StringTokenizer st;
            Table tb = new Table();
            if (this.ignoreFirst) {
                scanner.readLine().trim().split(delim);
            }
            String input = scanner.readLine().trim();
            int leng = input.trim().split(delim).length;
            int count = 0;
            int[][] fks = new int[this.tableSize.get(table)][fkSize];
            String[] nonKeys = new String[this.tableSize.get(table)];
            while (input != null) {
                st = new StringTokenizer(input.trim());
                int t = Integer.parseInt(st.nextToken())-this.leading;
                    for (int i = 1; i < leng && i < this.fkSize + 1; i++) {
                        fks[t][i - 1] = Integer.parseInt(st.nextToken())-this.leading;
                        if (fks[t][i - 1]<0){
                        fks[t][i - 1] = 0;
                        }
                    }
                    String nonkey = "";
                    for (int i = this.fkSize + 1; i<leng;i++){
                        if (st.hasMoreTokens()){
                        nonkey += "\t" + st.nextToken();}
                        else{
                            nonkey+="\t" + "null"; 
                        }
                    }
                    nonKeys[t] = nonkey.trim();
               //  } catch (Exception ne) {
               //     System.out.println(ne.fillInStackTrace());
               //     System.out.println(count + " : " + input + " Table: " + table);
               // }

                count++;
                input = scanner.readLine();

            }
            tb.fks = fks;
            tb.nonKeys = nonKeys;
            tb.tableName = table;
            tb.fkSize =fkSize; 
            tables[this.tableNum] = tb;
            tables[this.tableNum].tableName = this.table;
            System.out.println("Mem=====" + table + "   " + count + "    " + delim + "    " + leng + "  " + "====");
            
            scanner.close();
            scanner = null;
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ParaReader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ParaReader.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                input1.close();
            } catch (IOException ex) {
                Logger.getLogger(ParaReader.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

 
}
