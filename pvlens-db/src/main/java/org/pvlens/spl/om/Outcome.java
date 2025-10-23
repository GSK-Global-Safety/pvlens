package org.pvlens.spl.om;

/*
 * This file is part of PVLens.
 *
 * Copyright (C) 2025 GlaxoSmithKline
 *
 * PVLens is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PVLens is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with PVLens.  If not, see <https://www.gnu.org/licenses/>.
 */


import java.util.*;

import org.pvlens.spl.umls.Atom;

import lombok.Data;

@Data
public class Outcome {

    // An outcome contains the MedDRA codes associated
    private Set<Atom> codes;

    // Date the event was first added to the label
    private HashMap<String, Date> firstAdded;

    // Boolean flags for outcome classification
    private boolean blackbox;
    private boolean warning;
    private boolean indication;

    // Track the outcome source
    private HashMap<String, List<String>> outcomeSource;

    private boolean exactMatch;
    
    /**
     * Default constructor
     */
    public Outcome() {

        this.codes = new HashSet<>();
        this.firstAdded = new HashMap<>();

        this.blackbox = false;
        this.warning = false;
        this.indication = false;

        // Set the type of matches this outcome contains
        this.exactMatch = false;
        
        // Track where outcomes originate from
        this.outcomeSource = new HashMap<>();
    }

    /**
     * Update the date added to the label
     *
     * @param src     Source of the outcome (e.g., ZIP file name)
     * @param aui     Atom Unique Identifier (AUI)
     * @param newDate Date the outcome was added
     */
    public void updateDateAdded(String src, String aui, Date newDate) {
        if (src == null || aui == null) {
            return; // or log; nothing useful to do
        }

        // Only update the "firstAdded" map when we actually have a date
        if (newDate != null) {
            Date existingDate = this.firstAdded.get(aui);
            if (existingDate == null || newDate.before(existingDate)) {
                this.firstAdded.put(aui, newDate);
            }
        }

        // Always track the source even if the date is unknown yet
        this.outcomeSource.computeIfAbsent(src, k -> new ArrayList<>());
        List<String> list = this.outcomeSource.get(src);
        if (!list.contains(aui)) {
            list.add(aui);
        }
    }
    
    
    /**
     * Add a code to this outcome if not previously added, or update the date if it
     * already exists
     *
     * @param src       Source of the outcome (e.g., ZIP file name)
     * @param syn       The Atom object representing the outcome
     * @param labelDate Date the outcome was added
     */
    public void addCode(String src, Atom syn, Date labelDate) {
        codes.add(syn);
        updateDateAdded(src, syn.getAui(), labelDate);
    }

    /**
     * Get a list of sources where the outcome was found
     *
     * @param aui Atom Unique Identifier (AUI)
     * @return List of sources where the outcome was found
     */
    public List<String> getSources(String aui) {
        List<String> sources = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : this.outcomeSource.entrySet()) {
            if (entry.getValue().contains(aui)) {
                sources.add(entry.getKey());
            }
        }
        return sources;
    }

    /**
     * Add a code from multiple sources.
     *
     * @param sources   List of sources (e.g., ZIP file names)
     * @param atom      The Atom object representing the outcome
     * @param labelDate Date the outcome was added
     */
    public void addCode(List<String> sources, Atom atom, Date labelDate) {
        for (String src : sources) {
            this.addCode(src, atom, labelDate);
        }
    }

    // Overriding equals() to compare two Outcome objects
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Outcome)) {
            return false;
        }
        Outcome other = (Outcome) o;
        return blackbox == other.blackbox && warning == other.warning && indication == other.indication
                && codes.equals(other.codes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(codes, blackbox, warning, indication);
    }
}
