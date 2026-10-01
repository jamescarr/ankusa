package io.github.jamescarr.ankusa.conformance;

import java.util.List;

/**
 * One {@code conformance/cases/*.json} file.
 *
 * @param cases the vectors it holds
 */
record CaseFile(List<CaseSpec> cases) {}
