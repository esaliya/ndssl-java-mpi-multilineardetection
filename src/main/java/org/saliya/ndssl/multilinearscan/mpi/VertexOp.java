package org.saliya.ndssl.multilinearscan.mpi;

import java.util.List;

/**
 * Saliya Ekanayake on 4/3/17.
 */
public interface VertexOp {
    short apply(List<Message> msgs);
}
