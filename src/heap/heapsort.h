/*
 * heapsort.h
 *
 *  Created on: Mar 1, 2015
 *      Author: costantinos
 */

#ifndef HEAP_HEAPSORT_H_
#define HEAP_HEAPSORT_H_



/*-----------------------------------------------------------------------*/
/* The sift-up procedure sinks a hole from v[i] to leaf and then sifts   */
/* the original v[i] element from the leaf level up. This is the main    */
/* idea of bottom-up heapsort.                                           */
/*-----------------------------------------------------------------------*/
template <typename element,typename comparator>
static void siftup(
element v[], int i, int n,comparator comp) {
	int j, start;
	element x;

	start = i;
	x = v[i];
	j = i << 1;
	while (j <= n) {
		if (j < n)
			if (comp(v[j],v[j + 1]))
				j++;
		v[i] = v[j];
		i = j;
		j = i << 1;
	}
	j = i >> 1;
	while (j >= start) {
		if (comp(v[j],x)) {
			v[i] = v[j];
			i = j;
			j = i >> 1;
		} else
			break;
	}
	v[i] = x;
} /* End of siftup */

/*----------------------------------------------------------------------*/
/* The heapsort procedure; the original array is r[0..n-1], but here    */
/* it is shifted to vector v[1..n], for convenience.                     */
/*----------------------------------------------------------------------*/
template <typename element,typename comparator>
void bottom_up_heapsort(
element r[], int n,comparator comp) {
	int k;
	element x;
	element *v;

	v = r - 1; /* The address shift */

	/* Build the heap bottom-up, using siftup. */
	for (k = n >> 1; k > 1; k--)
		siftup(v, k, n,comp);

	/* The main loop of sorting follows. The root is swapped with the last  */
	/* leaf after each sift-up. */
	for (k = n; k > 1; k--) {
		siftup(v, 1, k,comp);
		x = v[k];
		v[k] = v[1];
		v[1] = x;
	}
} /* End of bottom_up_heapsort */



#endif /* HEAP_HEAPSORT_H_ */
