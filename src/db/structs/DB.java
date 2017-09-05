package db.structs;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ZhangJiangwei
 */
public class DB {
    HashMap<String, AttributeType> groupType = new HashMap<>();
    public HashMap<String, String[]> groupData = new HashMap<>();
    public Table[] tables;
    public HashMap<String, Integer> tableMapping = new HashMap<>(); //tableMapping

    public HashMap<String, Integer> tableSize = new HashMap<>();
    public HashMap<String, Integer> fkSize = new HashMap<>();
    public HashMap<String, String> tableType = new HashMap<>();
    public HashMap<String, ArrayList<String>> table_attr_map = new HashMap<>();
    public HashMap<String, ArrayList<ComKey>> fkRelation = new HashMap<>();
    public HashMap<String, ArrayList<ComKey>> mergedDegreeTitle = new HashMap<>();
    public HashMap<ComKey, Integer> comKeyMap = new HashMap<>();  //chainKey_map
    public HashMap<String, HashMap<Integer, String>> groupAttributePositionMap = new HashMap<>();
    public HashMap<String, HashMap<Integer, AttributeType>> typeAttributePositionMap = new HashMap<>();
    private String configurationFile;
    public HashMap<String, ArrayList<String>> restructedTableCKMap = new HashMap();
    public HashMap<String, ArrayList<String>> referencingTables = new HashMap<>();
    public ArrayList<String> getReferencingTables(String table){
        if (referencingTables.containsKey(table)){
            return referencingTables.get(table);
        }
        ArrayList<String> tables = new ArrayList<>();
        if (fkRelation.containsKey(table)){
            for (ComKey ck: fkRelation.get(table)){
                tables.add(ck.sourceTable);
            }
        }
        referencingTables.put(table, tables);
        return referencingTables.get(table); 
    }
    
    public void producessTableCKMap() {
        for (String table : fkRelation.keySet()) {
            ArrayList<String> arrayList = new ArrayList<>();
            for (ComKey ck : fkRelation.get(table)) {
                arrayList.add(tableType.get(ck.sourceTable));
            }
            restructedTableCKMap.put(table, arrayList);
        }
    }

    public int getTableID(String table) {
        return tableMapping.get(table);
    }

    public void dropFKs() {
        for (Table t : tables) {
            t.fks = null;
        }
    }
    
    public String[] getTableNonKeyString(int tableID){
         return tables[tableID].nonKeys;
    }
    
    public void outputRandomFKDB(String suffix, String outputDir, String loadOption){
        for (Table table: tables){
            ParaDBWriter paraWriter = new ParaDBWriter(table, loadOption, outputDir, suffix, Options.randomFK);
            Thread thread = new Thread(paraWriter);
            thread.start();
        }
    
    }
    
    public void outputDB(String suffix, String outputDir, String loadOption){
        for (Table table: tables){
            ParaDBWriter paraWriter = new ParaDBWriter(table, loadOption, outputDir, suffix);
            Thread thread = new Thread(paraWriter);
            thread.start();
        }
    }
    
    public String searchParitionGroupName() {
        for (String groupName : groupType.keySet()) {
            if (groupType.get(groupName).equals(AttributeType.PARTITION_TIMESTAMP)) {
                return groupName;
            }
        }
        return null;
    }

    public void loadMap(String configFile) {
        try {
            Scanner scanner = new Scanner(new File(configFile));
            this.configurationFile = configFile;
            while (scanner.hasNext()) {
                String[] splits = scanner.nextLine().trim().split("\\s+");
                int num = Integer.parseInt(splits[1]);
                String tableName = splits[0];
                this.fkSize.put(tableName, num);
                this.tableSize.put(tableName, Integer.parseInt(splits[2]));

                if (splits.length > 3) {
                    this.tableType.put(tableName, splits[3]);
                }
                String[] attributes = scanner.nextLine().split("\\s+");
                ArrayList<String> arr = new ArrayList<>();
                for (int i = 0; i < attributes.length; i++) {
                    arr.add(attributes[i]);
                }
                table_attr_map.put(tableName, arr);
            }
            scanner.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    //Return the map which indicates the FK referencing relation. Key is the table name, Value is the referenced table and the corrspoondoing index.
    public void load_fkRelation() {
        for (Map.Entry<String, ArrayList<String>> table : table_attr_map.entrySet()) {
            String tName = table.getKey();
            ArrayList<String> fks = new ArrayList<>();

            for (int i = 1; i < table.getValue().size(); i++) {
                String aName = table.getValue().get(i);
                if (aName.contains("-")) {
                    String[] temp = aName.split("-");
                    //table_attr_map.get(tName).set(i, temp[0]);
                    String[] temps = temp[1].split(":");
                    ComKey comkey = new ComKey();
                    comkey.sourceTable = temps[0];
                    comkey.referencingTable = tName;
                    comkey.referenceposition = i;
                    int chainKey_size = comKeyMap.size();
                    comKeyMap.put(comkey, chainKey_size);

                    if (!(tName.equals("socialgraph") && i == 2)) {
                        fks.add(temps[0]);
                        if (!this.fkRelation.containsKey(tName)) {
                            fkRelation.put(tName, new ArrayList<ComKey>());
                        }
                        if (!fkRelation.get(tName).contains(comkey)) {
                            fkRelation.get(tName).add(comkey);
                        }
                    } else {
                        fkRelation.get(tName).add(fkRelation.get(tName).get(0));
                    }
                }
            }

        }

    }
    
    /**
     * Load file into memory
     * @param filePath
     * @param leading
     * @param ignoreFirst
     * @param delim
     * @param loadOption
     * @param suffix
     * @param configurationFile 
     */
    public void initial_loading(String filePath, int leading, boolean ignoreFirst, String delim, String loadOption,
            String suffix, String configurationFile) {
        long startTime = System.currentTimeMillis();

        try {
            loadMap(configurationFile);
            System.out.println("====================Map Loaded=============================");
            load_fkRelation();
            System.out.println("=========================loadTuples=======================");

            processMergeDegreeTitle();
            if (loadOption.equals(Options.loadFull)) {
                parseNonKeyTypes(table_attr_map);
            }
            loadTuple(filePath, leading, ignoreFirst, delim, loadOption, suffix);
            for (String table:fkRelation.keySet()){
                int tableID = getTableID(table);
                tables[tableID].fkMaximum = new int[fkRelation.get(table).size()];
                for (int i=0; i < fkRelation.get(table).size(); i++){
                    ComKey ck = fkRelation.get(table).get(i);
                    String fkTable = ck.sourceTable;
                    tables[tableID].fkMaximum[i] = tableSize.get(fkTable);
                }
            }
            producessTableCKMap();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DB.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(DB.class.getName()).log(Level.SEVERE, null, ex);
        }
        long endTime = System.currentTimeMillis();
        System.err.println("loadingTime: " + (endTime - startTime)/1000 );
    }

    private AttributeType parseStringType(String split) {
        switch (split) {
            case "D":
                return AttributeType.DISCRETE;
            case "C":
                return AttributeType.CATEGORY;
            case "T":
                return AttributeType.TIMESTAMP;
            case "S":
                return AttributeType.STRING;
            case "P":
                return AttributeType.PARTITION_TIMESTAMP;
        }

        System.err.println("The type is WRONG!!! \t " + split);
        System.exit(-1);
        return null;
    }

    private void parseNonKeyTypes(HashMap<String, ArrayList<String>> maps) {

        for (Map.Entry<String, ArrayList<String>> table : maps.entrySet()) {
            int skip = 0;
            if (fkSize.containsKey(table.getKey())) {
                skip = fkSize.get(table.getKey());
            }

            for (int i = 1 + skip; i < table.getValue().size(); i++) {
                String attributeName = table.getValue().get(i);

                String[] splits = attributeName.trim().split(":");
                AttributeType type = parseStringType(splits[0]);
                String tableName = table.getKey();
                String group = "" + System.currentTimeMillis();
                if (splits.length > 2) {
                    group = splits[1];
                }
                if (!groupAttributePositionMap.containsKey(tableName)) {
                    groupAttributePositionMap.put(tableName, new HashMap<Integer, String>());
                    typeAttributePositionMap.put(tableName, new HashMap<Integer, AttributeType>());
                }
                groupAttributePositionMap.get(tableName).put(i - 1 - skip, group);
                typeAttributePositionMap.get(tableName).put(i - 1 - skip, type);
                groupType.put(group, type);
            }
        }
    }

    public void loadTuple(String filePath, int leading, boolean ignoreFirst, String delim, String loadOption, String suffix) throws FileNotFoundException, IOException {
        ArrayList<Thread> liss = new ArrayList<>();
        this.tables
                = new Table[this.table_attr_map.size()];
        int count = 0;
        for (String table : this.table_attr_map.keySet()) {
            ReadFullTable pr = new ReadFullTable(table, ignoreFirst, count, tableSize, leading, delim, loadOption,
                    fkSize.get(table), filePath, suffix, tables);
            this.tableMapping.put(table, count);
            count++;
            Thread thr = new Thread(pr);
            liss.add(thr);
            thr.start();
        }
        for (Thread thr : liss) {
            try {
                thr.join();
            } catch (InterruptedException ex) {
            }
        }
    }

    public void processMergeDegreeTitle() {
        HashSet<ComKey> temp = new HashSet<>();
        for (ArrayList<ComKey> cks : fkRelation.values()) {
            temp.addAll(cks);
        }
        for (ComKey key : temp) {
            if (!mergedDegreeTitle.containsKey(key.sourceTable)) {
                mergedDegreeTitle.put(key.sourceTable, new ArrayList<ComKey>());
            }
            if (!mergedDegreeTitle.get(key.sourceTable).contains(key)) {
                mergedDegreeTitle.get(key.sourceTable).add(key);
            }
        }
    }

    public void outputConfiguration(String outStr, int[] printedNum) {
        try {
            PrintWriter pw = new PrintWriter(new File(outStr + "/" + configurationFile));
            for (String tableName : table_attr_map.keySet()) {
                int tableNum = getTableID(tableName);
                pw.println(tableName + "\t" + fkSize.get(tableName) + "\t" + printedNum[tableNum]);
                String line = "";
                for (String split : table_attr_map.get(tableName)) {
                    line += "\t" + split;
                }
                pw.println(line.trim());
            }
            pw.close();

        } catch (FileNotFoundException ex) {
            Logger.getLogger(DB.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void loadMappedData(String inStr) {
        for (String grounName : groupType.keySet()) {
            try {
                Scanner scanner = new Scanner(new File(inStr + "/" + grounName + Options.outputMapSuff));
                int num = Integer.parseInt(scanner.nextLine().trim());
                String[] map = new String[num];
                while (scanner.hasNext()) {
                    String[] splits = scanner.nextLine().trim().split("\\s+");
                    int id = Integer.parseInt(splits[1]);
                    map[id] = splits[0];
                }
                scanner.close();
                groupData.put(grounName, map);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(DB.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public String getTableType(String tableName) {
        return tableType.get(tableName);
    }

    public int getNumberOfTables() {
        return tableSize.size();
    }

    public ArrayList<ComKey> getTableFKs(String editingTable) {
        return fkRelation.get(editingTable);
    }

    public int[] getTableFKValues(int editingTableID, int pid) {
        return tables[editingTableID].fks[pid];
    }

    public int getTableSize(String table) {
        return tableSize.get(table);
    }

    public Set<String> getAllTables() {
        return tableSize.keySet();
    }

    public int[][] getTableNonKeyValues(int tableID) {
        return tables[tableID].fullNonKeys;
    }
    
    

    public int[] getTableColumn(int tableID, int tableCol) {
        int[] result = new int[tables[tableID].fullNonKeys.length];
        for (int i=0; i < result.length; i++){
            result[i] = tables[tableID].fullNonKeys[i][tableCol];
        }
        return result;
    }

    public int getNonKeyTableLength(int tableID) {
        return tables[tableID].fullNonKeys.length;
    }
    
    public int getTableNonKeyLength(int tableID){
        if ( tables[tableID].fullNonKeys != null && tables[tableID].fullNonKeys.length>1){
            if (tables[tableID].fullNonKeys[0]!=null){
                return tables[tableID].fullNonKeys[0].length;
            }
        }
        return 0;
    }

    public int getTableColumnNumber(int tableID, int tableCol, int i) {
        return tables[tableID].fullNonKeys[i][tableCol];
    }

    public int getMaxNonkeyNumber(int tableID, int tableCol) {
        return tables[tableID].nonKeyMaxNum[tableCol];
    }

    public void updateTableNonKey(int tableID, int[] indexes, int[] values, int pid) {
        for (int i = 0; i < indexes.length; i++){
            tables[tableID].fullNonKeys[pid][indexes[i]] = values[i]; 
        }
    }

    public int[] getMaxNumber(int tableID, int[] indexes) {
        int[] maxNumber = new int[indexes.length];
        for (int i =0; i< indexes.length; i++){
            maxNumber[i] = tables[tableID].nonKeyMaxNum[indexes[i]];
        }
        return maxNumber;
    }

    public Set<ComKey> getComKeys() {
        return comKeyMap.keySet();
    }
    
    public int[] getFKValues(int tableID, int tupleID){
        return tables[tableID].fks[tupleID];
    }

    public String getTableName(int tableID) {
        return tables[tableID].tableName;
    }
    

    

}
