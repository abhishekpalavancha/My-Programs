package simpledb;

import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	/**
	 * The Types defined in this TupleDesc.
	 */
	protected Type types[];

	/**
	 * The names defined in this TupleDesc.
	 */
	protected String names[];

	/**
	 * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields, with the first td1.numFields coming
	 * from td1 and the remaining from td2.
	 * 
	 * @param td1
	 *            The TupleDesc with the first fields of the new TupleDesc
	 * @param td2
	 *            The TupleDesc with the last fields of the TupleDesc
	 * @return the new TupleDesc
	 */
	public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
		Type[] newTypes = new Type[td1.numFields() + td2.numFields()];
		String[] newNames = new String[td1.numFields() + td2.numFields()];
		for (int i = 0; i < td1.numFields(); i++) {
			newTypes[i] = td1.getType(i);
			newNames[i] = td1.getFieldName(i);
		}
		for (int i = 0; i < td2.numFields(); i++) {
			newTypes[i + td1.numFields()] = td2.getType(i);
			newNames[i + td1.numFields()] = td2.getFieldName(i);
		}
		return new TupleDesc(newTypes, newNames);
	}

	/**
	 * Create a new TupleDesc with typeAr.length fields with fields of the specified types, with associated named
	 * fields.
	 * 
	 * @param typeAr
	 *            array specifying the number of and types of fields in this TupleDesc. It must contain at least one
	 *            entry.
	 * @param fieldAr
	 *            array specifying the names of the fields. Note that names may be null.
	 */
	public TupleDesc(Type[] typeAr, String[] fieldAr) {
		types = typeAr;
		names = fieldAr;
	}

	/**
	 * Constructor. Create a new tuple desc with typeAr.length fields with fields of the specified types, with anonymous
	 * (unnamed) fields.
	 * 
	 * @param typeAr
	 *            array specifying the number of and types of fields in this TupleDesc. It must contain at least one
	 *            entry.
	 */
	public TupleDesc(Type[] typeAr) {
		types = typeAr;
		names = new String[typeAr.length];
	}

	/**
	 * @return the number of fields in this TupleDesc
	 */
	public int numFields() {
		return types.length;
	}

	/**
	 * Gets the (possibly null) field name of the ith field of this TupleDesc.
	 * 
	 * @param i
	 *            index of the field name to return. It must be a valid index.
	 * @return the name of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public String getFieldName(int i) throws NoSuchElementException {
		if (i >= 0 && i < names.length)
			return names[i];
		throw new NoSuchElementException("invalid field index");
	}

	/**
	 * Find the index of the field with a given name.
	 * 
	 * @param name
	 *            name of the field.
	 * @return the index of the field that is first to have the given name.
	 * @throws NoSuchElementException
	 *             if no field with a matching name is found.
	 */
	public int nameToId(String name) throws NoSuchElementException {
		if (names == null)
			throw new NoSuchElementException("no named fields");
		for (int i = 0; i < names.length; i++) {
			if (names[i] != null && names[i].equals(name)) {
				return i;
			}
		}
		throw new NoSuchElementException("invalid field name: " + name);
	}

	/**
	 * Gets the type of the ith field of this TupleDesc.
	 * +++
	 * @param i
	 *            The index of the field to get the type of. It must be a valid index.
	 * @return the type of the ith field
	 * @throws NoSuchElementException
	 *             if i is not a valid field reference.
	 */
	public Type getType(int i) throws NoSuchElementException {
		if(i<0 || i>this.numFields())
			throw new NoSuchElementException("INVLID INDEX");
		else
			return types[i];
			// some code goes here
 	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two TupleDescs are considered equal if they are
	 * the same size and if the n-th type in this TupleDesc is equal to the n-th type in td.
	 * 
	 * @param o
	 *            the Object to be compared for equality with this TupleDesc.
	 * @return true if the object is equal to this TupleDesc.
	 */
	public boolean equals(Object o) {
		if (!(o instanceof TupleDesc))
			return false;
		return equals((TupleDesc) o);
	}

	/**
	 * Compares the specified object with this TupleDesc for equality. Two TupleDescs are considered equal if they are
	 * the same size and if the n-th type in this TupleDesc is equal to the n-th type in td.
	 * 
	 * @param o
	 *            the TupleDesc to be compared for equality with this TupleDesc.
	 * @return true if the TupleDesc is equal to this TupleDesc.
	 */
	public boolean equals(TupleDesc o) {
		if (types.length != o.types.length)
			return false;
		for (int i = 0; i < types.length; i++)
			if (!types[i].equals(o.types[i]))
				return false;
		return true;
	}

	/**
	 * Returns a String describing this descriptor. It should be of the form
	 * "fieldType[0](fieldName[0])|fieldType[1](fieldName[1])|...|fieldType[M](fieldName[M])".
	 * 
	 * @return String describing this descriptor.
	 */
	public String toString() {
		// some code goes here
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < types.length; i++) 
		{

            sb.append(types[i].toString());
            if (names != null)
                sb.append('(').append(names[i]).append(')');
            if (i != (types.length - 1))
                sb.append('|');
        }
        	return sb.toString();
        	//throw new UnsupportedOperationException("Implement this");
	}
}
