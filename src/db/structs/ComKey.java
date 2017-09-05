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
public class ComKey {
   public String sourceTable;
   public String referencingTable;

    public String getSourceTable() {
        return sourceTable;
    }

    public int getReferenceposition() {
        return referenceposition;
    }
   public int referenceposition;

    @Override
    public String toString() {
      return this.sourceTable+"\t"+this.referencingTable;
    }

    public String getReferencingTable() {
        return referencingTable;
    }
    
}
