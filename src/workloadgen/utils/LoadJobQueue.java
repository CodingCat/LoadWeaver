package workloadgen.utils;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class LoadJobQueue<E> implements Queue<E> {
	
	private LinkedList<E> list = new LinkedList<E>();
	
	@Override
	public boolean addAll(Collection<? extends E> values) {
		for (E e : values){
			list.add(e);
		}
		return true;
	}

	@Override
	public void clear() {
		list.clear();
	}

	@Override
	public boolean contains(Object o) {
		return list.contains(o);
	}

	@Override
	public boolean containsAll(Collection<?> values) {
		for (Object e : values){
			if (list.contains(e) == false){
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean isEmpty() {
		return list.isEmpty();
	}

	@Override
	public Iterator<E> iterator() {
		return list.iterator();
	}

	@Override
	public boolean remove(Object o) {
		return list.remove(o);
	}

	@Override
	public boolean removeAll(Collection<?> values) {
		for (Object o : values){
			if (list.remove(o) == false){
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean retainAll(Collection<?> values) {
		for (Object o : values){
			if (list.contains(o) == false){
				list.remove(o);
			}
		}
		return true;
	}

	@Override
	public int size() {
		return list.size();
	}

	@Override
	public Object[] toArray() {
		Object[] array = list.toArray();
		return array;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return list.toArray(a);
	}

	@Override
	public boolean add(E e) {
		return list.add(e);
	}

	@Override
	public E element() {
		if (list.isEmpty()) return null;
		return list.get(0);
	}

	@Override
	public boolean offer(E e) {
		return list.add(e);
	}

	@Override
	public E peek() {
		if (list.isEmpty()) return null;
		return list.get(0);
	}

	@Override
	public E poll() {
		if (list.isEmpty()) return null;
		return list.remove(0);
	}

	@Override
	public E remove() {
		if (list.isEmpty()) return null;
		return list.remove(0);
	}
	
	public E tail(){
		if (list.isEmpty()) return null;
		return list.get(list.size() - 1);
	}
}
