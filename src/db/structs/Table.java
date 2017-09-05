package db.structs;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author Zhang Jiangwei
 */
//By default, the first attribute is PK, and it is integer
public class Table {
   public int[][] fks; //exclude pk
   public String[] nonKeys;//
   public int[][] fullNonKeys;
   public String[] attributeNames; //nonKey Attributes
   public AttributeType[] attributeTypes;
   public String tableName;
   public int fkSize; //fk size
   public int[] nonKeyMaxNum;
   public int[] fkMaximum;
  
}
