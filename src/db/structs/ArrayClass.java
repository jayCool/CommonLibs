package db.structs;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Zhang Jiangwei
 */

public class ArrayClass {
    public int[] fks;

    public void initialize_FK(DB originalDB, String refTable, HashMap<Integer, ArrayList<Integer>> map) {
        fks = new int[originalDB.tableSize.get(refTable)];
            for (Map.Entry<Integer, ArrayList<Integer>> entry :map.entrySet()) {
                for (int i : entry.getValue()) {
                    fks[i] = entry.getKey();
                }
            }
    }
}
