/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package db.structs;

/**
 *
 * @author Zhang Jiangwei
 */
public class AttributeKey {

    public String table;
    public int attributeIndex;
    
    public String toString(){
        return table + "-" + attributeIndex;
    }
}
