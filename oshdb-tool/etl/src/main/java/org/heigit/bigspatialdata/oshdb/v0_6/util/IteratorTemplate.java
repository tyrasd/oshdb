package org.heigit.bigspatialdata.oshdb.v0_6.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class IteratorTemplate<T> implements Iterator<T>{
	
	private T next;
	private Exception e;
	
	
	@Override
	public boolean hasNext() {
		try {
			return next != null || (next = getNext()) != null;
		} catch (Exception e) {
			this.e = e;
		}		
		return false;
	}
	
	@Override
	public T next() {
		if(!hasNext()){
			throw new NoSuchElementException((e != null)?e.getMessage():null);
		}
		T ret = next;
		next = null;
		return  ret;
	}
	
	public Exception getException(){
		return e;
	}
	
	public void throwException() throws Exception{
		if(e != null)
			throw e;
	}
	
	
	protected void setNext(T next){
		this.next = next;
	}
	
	protected abstract T getNext() throws Exception;
	
}
