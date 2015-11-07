package simpledb;

/** Predicate compares tuples to a specified Field value.
 */
public class Predicate {

    /** Constants used for return codes in Field.compare */
    public enum Op {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
    }
    
	protected int field;
	
    protected Op op;
    
    protected Field operand;

    /**
     * Constructor.
     *
     * @param field field number of passed in tuples to compare against.
     * @param op operation to use for comparison
     * @param operand field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
    	this.field = field;
    	this.op = op;
    	this.operand = operand;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific
     * in the constructor.  The comparison can be made through Field's
     * compare method.
     *
     * @param t The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
    	Field f = t.getField (field);
    	if (f != null)
    		return f.compare(op, operand);
        return false;
    }

    /**
     * Returns something useful, like
     * "f = this.fieldid op = this.opstring operand = this.operandstring
     */
    public String toString() {
    	return String.format("f = %i op = %s operand = %s", field, op, operand);
    }
}
