/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package db.structs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Zhang-Jiangwei
 */
class ParaDBWriter implements Runnable {

    Table table;
    String loadOption;
    String outputDir;
    String suffix;
    String randomFKOption;

    ParaDBWriter(Table table, String loadOption, String outputDir, String suffix) {
        this.table = table;
        this.loadOption = loadOption;
        this.outputDir = outputDir;
        this.suffix = suffix;
    }

    ParaDBWriter(Table table, String loadOption, String outputDir, String suffix, String randomFKOption) {
        this.table = table;
        this.loadOption = loadOption;
        this.outputDir = outputDir;
        this.suffix = suffix;
        this.randomFKOption = randomFKOption;
    }

    @Override
    public void run() {
        if (randomFKOption.equals(Options.randomFK)) {
            PrintWriter pw = null;
            Random random = new Random();
            try {
                pw = new PrintWriter(new File(outputDir + File.separator + table.tableName + suffix));
                for (int pid = 0; pid < table.fks.length; pid++) {
                    String line = "" + pid;
                    for (int j = 0; j < table.fks[pid].length; j++) {
                        line += "\t" + random.nextInt(table.fkMaximum[j]);
                    }
                    if (loadOption.equals(Options.loadFK)) {
                        line += "\t" + table.nonKeys[pid];
                    } else {
                        for (int j = 0; j < table.fullNonKeys[pid].length; j++) {
                            line += "\t" + table.fullNonKeys[pid][j];
                        }
                    }
                    pw.println(line);
                }
            } catch (FileNotFoundException ex) {
                Logger.getLogger(ParaDBWriter.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                pw.close();
            }
        } else {
            PrintWriter pw = null;
            try {
                pw = new PrintWriter(new File(outputDir + File.separator + table.tableName + suffix));
                for (int pid = 0; pid < table.fks.length; pid++) {
                    String line = "" + pid;
                    for (int j = 0; j < table.fks[pid].length; j++) {
                        line += "\t" + table.fks[pid][j];
                    }
                    if (loadOption.equals(Options.loadFK)) {
                        line += "\t" + table.nonKeys[pid];
                    } else {
                        for (int j = 0; j < table.fullNonKeys[pid].length; j++) {
                            line += "\t" + table.fullNonKeys[pid][j];
                        }
                    }
                    pw.println(line);
                }
            } catch (FileNotFoundException ex) {
                Logger.getLogger(ParaDBWriter.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                pw.close();
            }
        }
    }

}
