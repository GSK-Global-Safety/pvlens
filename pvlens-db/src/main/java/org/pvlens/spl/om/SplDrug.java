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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.pvlens.spl.umls.Atom;
import org.pvlens.spl.umls.UmlsLoader;

import lombok.Data;

@Data
public class SplDrug {

	private int id;

	//
	// All below will be linked via UMLS CUI
	// assignments
	//
	private String guid;
	private int sourceType;

	// Corresponding XML files assigned to this product
	private HashMap<String, Boolean> xmlFiles;

	private HashMap<String, List<String>> mergedGuidXmlPairs;
	private HashMap<String, Date> guidApprovalDate;
	private HashMap<String, Integer> guidNda; // NDA will allow us to link SRLC data

	// Associated atoms
	private HashMap<String, Atom> drugProduct; // direct SPL GUID to MTHSPL entries
	private HashMap<String, Atom> snomedPts; // map from MTHSPL to SNOMED
	private HashMap<String, Atom> ingredients; // From SNOMED, find associated ingredient level
	private HashMap<String, Atom> rxNormPts; // Link from SNOMED, MTHSPL to RxNorm
	private HashMap<String, Atom> atcCodes; // Link from NDC Code to ATC code

	private Set<String> rawNdcCodes; // used in initial loading of SPL data

	// Link SRLC data
	private HashMap<Integer, Srlc> srlcs;

	// Useful for grouping related SPL entries
	private List<String> drugProductCuis;
	private List<String> snomedParentAuis;

	// Extracted outcomes from XML
	private Outcome exactMatchWarnings;
	private Outcome exactMatchBlackbox;
	private Outcome exactMatchIndications;

	private Outcome nlpMatchWarnings;
	private Outcome nlpMatchBlackbox;
	private Outcome nlpMatchIndications;

	// Last label date procesesd
	Date labelDate;
	
	// Drug Sponsor (from NDA data)
	String drugSponsor;

	// Should we save this product?
	// This flag will be set to false
	// if merged with another product
	private boolean save;

	public SplDrug() {

		// Initialize the product ID for db storage
		this.id = -1;
		this.sourceType = -1;

		// Initialize maps
		this.drugProduct = new HashMap<>();
		this.snomedPts = new HashMap<>();
		this.ingredients = new HashMap<>();
		this.rxNormPts = new HashMap<>();
		this.atcCodes = new HashMap<>();
		this.xmlFiles = new HashMap<>();
		this.rawNdcCodes = new HashSet<>();

		this.mergedGuidXmlPairs = new HashMap<>();
		this.guidApprovalDate = new HashMap<>();
		this.guidNda = new HashMap<>();
		this.srlcs = new HashMap<>();

		this.drugProductCuis = new ArrayList<>();
		this.snomedParentAuis = new ArrayList<>();

		// Initialize outcomes
		this.exactMatchWarnings = new Outcome();
		this.exactMatchBlackbox = new Outcome();
		this.exactMatchIndications = new Outcome();

		this.nlpMatchWarnings = new Outcome();
		this.nlpMatchBlackbox = new Outcome();
		this.nlpMatchIndications = new Outcome();

		this.drugSponsor = "";
		this.labelDate = null;
		this.save = true;
	}

	/**
	 * Get multi-cui keyset
	 * 
	 * @return
	 */
	public String getMultiCuiKey() {
		StringBuilder output = new StringBuilder();
		Collections.sort(this.getDrugProductCuis());
		for (String cui : this.getDrugProductCuis()) {
			output.append(cui);
			output.append("|");
		}
		return output.toString();
	}

	/**
	 * An entry may contain multiple parents - we can check later to merge
	 * 
	 * @param aui
	 */
	public void addSnomedParent(String aui) {
		if (!this.snomedParentAuis.contains(aui)) {
			this.snomedParentAuis.add(aui);
		}
	}

	public void resetParents() {
		this.snomedParentAuis = new ArrayList<String>();
	}

	public void addDrugProductCui(String cui) {
		if (this.drugProductCuis.contains(cui) == false) {
			this.drugProductCuis.add(cui);
		}
		return;
	}

	public void addSnomedPt(Atom pt) {
		this.snomedPts.put(pt.getAui(), pt);
	}

	public void addRxNormPt(Atom pt) {
		this.rxNormPts.put(pt.getAui(), pt);
	}

	public void addIngedient(Atom ing) {
		this.ingredients.put(ing.getAui(), ing);
	}

	// Compute the set of source types from our XML paths (including merged ones)
	public java.util.Set<SourceType> getSourceTypes() {
		java.util.Set<SourceType> out = new java.util.HashSet<>();
		// own xmls
		for (String xml : getXmlFilesAsList()) {
			out.add(SourceType.fromPath(xml));
		}
		// merged xmls
		if (this.mergedGuidXmlPairs != null) {
			for (java.util.List<String> files : this.mergedGuidXmlPairs.values()) {
				if (files == null)
					continue;
				for (String xml : files)
					out.add(SourceType.fromPath(xml));
			}
		}
		if (out.isEmpty())
			out.add(SourceType.UNKNOWN);
		return out;
	}

	private boolean hasSameSourceTypes(SplDrug other) {
		if (this.getSourceType() == other.getSourceType())
			return true;
		return false;
	}

	/**
	 * Update the labels with the labels from matching product NDC codes, generics,
	 * and outcomes.
	 * 
	 * @param prd the product to merge
	 */
	public void updateLabels(SplDrug prd) {

		// Update product outcomes
		updateOutcome(this.exactMatchWarnings, prd.getExactMatchWarnings(), false);
		updateOutcome(this.exactMatchBlackbox, prd.getExactMatchBlackbox(), false);
		updateOutcome(this.exactMatchIndications, prd.getExactMatchIndications(), false);
		updateOutcome(this.nlpMatchWarnings, prd.getNlpMatchWarnings(), false);
		updateOutcome(this.nlpMatchBlackbox, prd.getNlpMatchBlackbox(), false);
		updateOutcome(this.nlpMatchIndications, prd.getNlpMatchIndications(), false);
	}

	/**
	 * Iterate over all Atoms and compare the date first added
	 * 
	 * @param currentOutcome
	 * @param newOutcome
	 */
	private void updateOutcome(Outcome currentOutcome, Outcome newOutcome, boolean srlcUpdate) {

		if (srlcUpdate == false) {
			// Use SPL GUID to update sources
			for (Atom atom : newOutcome.getCodes()) {
				List<String> sources = newOutcome.getSources(atom.getAui());
				currentOutcome.addCode(sources, atom, newOutcome.getFirstAdded().get(atom.getAui()));
			}
		} else {
			// Preserve the SPL GUID as the source if possible, and simply update the date
			// added
			for (Atom atom1 : currentOutcome.getCodes()) {
				for (Atom atom2 : newOutcome.getCodes()) {
					if (atom1.getAui().contentEquals(atom2.getAui())) {
						List<String> currentSource = currentOutcome.getSources(atom1.getAui());
						currentOutcome.addCode(currentSource, atom1, newOutcome.getFirstAdded().get(atom1.getAui()));
					}
				}
			}

			// Are there any new atoms?
			for (Atom atom1 : newOutcome.getCodes()) {
				if (currentOutcome.getCodes().contains(atom1) == false) {
					// Logger.log("SRLC Adding new AE: " + atom1.getAui());
					// Just assign it to the current GUID so we can insure the database build
					// succeeds
					currentOutcome.addCode(this.getGuid(), atom1, newOutcome.getFirstAdded().get(atom1.getAui()));
				}
			}
		}

		reconcileOutcomeMinDates(currentOutcome);

	}
	
	
	// Prefer PT code if available; fall back to 'code' for true PTs.
	private static String ptKey(Atom a) {
	    String pt = a.getPtCode();
	    return (pt != null && !pt.isEmpty()) ? pt : a.getCode();
	}

	/** O(n) in-bucket reconciliation: propagate earliest date across entries
	 * that share either the same CUI or the same PT code (LLT↔PT).
	 */
	public static void reconcileOutcomeMinDates(Outcome out) {
	    if (out == null || out.getCodes() == null || out.getCodes().isEmpty()) return;

	    Map<String, Date> minByCui = new HashMap<>();
	    Map<String, Date> minByPt  = new HashMap<>();

	    // 1) Scan once: compute minima by CUI and by PT key
	    for (Atom a : out.getCodes()) {
	        if (a == null) continue;
	        Date d = out.getFirstAdded().get(a.getAui());
	        if (d == null) continue;

	        String cui = a.getCui();
	        if (cui != null && !cui.isEmpty()) {
	            minByCui.merge(cui, d, (oldD, newD) -> newD.before(oldD) ? newD : oldD);
	        }
	        String pk = ptKey(a);
	        if (pk != null && !pk.isEmpty()) {
	            minByPt.merge(pk, d, (oldD, newD) -> newD.before(oldD) ? newD : oldD);
	        }
	    }

	    // 2) Push the best min back to each AUI
	    for (Atom a : out.getCodes()) {
	        if (a == null) continue;
	        Date curr = out.getFirstAdded().get(a.getAui());

	        Date best = curr;
	        Date byCui = (a.getCui() != null) ? minByCui.get(a.getCui()) : null;
	        if (byCui != null && (best == null || byCui.before(best))) best = byCui;

	        Date byPt = minByPt.get(ptKey(a));
	        if (byPt != null && (best == null || byPt.before(best))) best = byPt;

	        if (best != null && (curr == null || best.before(curr))) {
	            out.getFirstAdded().put(a.getAui(), best);
	        }
	    }
	}


	/**
	 * Set unique labeled events Note, we may have AEs that are both warnings and
	 * blackbox however, we want to preserve the original dates it was added to each
	 * section of the label. We will assume in any post-analysis, that if it was
	 * moved from warning to black-box, the date should be after it was added to the
	 * warning.
	 */
	public void resolveLabeledEvents() {

		if (this.exactMatchIndications != null && this.exactMatchWarnings != null && this.exactMatchBlackbox != null) {

			//
			// If the AE is listed in the indication, remove from warnings
			//
			Set<Atom> exactIndication = exactMatchIndications.getCodes();
			exactMatchWarnings.getCodes().removeIf(exactIndication::contains);
			nlpMatchWarnings.getCodes().removeIf(exactIndication::contains);
			exactMatchBlackbox.getCodes().removeIf(exactIndication::contains);
			nlpMatchBlackbox.getCodes().removeIf(exactIndication::contains);
			nlpMatchIndications.getCodes().removeIf(exactIndication::contains);

			// Do the same for NLP matched indications
			Set<Atom> nlpIndication = nlpMatchIndications.getCodes();
			exactMatchWarnings.getCodes().removeIf(nlpIndication::contains);
			nlpMatchWarnings.getCodes().removeIf(nlpIndication::contains);
			exactMatchBlackbox.getCodes().removeIf(nlpIndication::contains);
			nlpMatchBlackbox.getCodes().removeIf(nlpIndication::contains);

			// Remove exact matches from NLP matches
			Set<Atom> exactBlackBox = exactMatchBlackbox.getCodes();
			nlpMatchBlackbox.getCodes().removeIf(exactBlackBox::contains);

			// Remove exact matches from NLP matches
			Set<Atom> exactWarning = exactMatchWarnings.getCodes();
			nlpMatchWarnings.getCodes().removeIf(exactWarning::contains);

			// Once resolved, remove any missing codes from first added
			cleanupFirstAdded();

		}
	}

	/**
	 * Cleanup date first added after code consolidation
	 */
	private void cleanupFirstAdded() {

		// All of our outcomes
		Outcome[] outcomes = new Outcome[] { exactMatchIndications, exactMatchWarnings, exactMatchBlackbox,
				nlpMatchIndications, nlpMatchWarnings, nlpMatchBlackbox };

		for (Outcome outcome : outcomes) {

			HashMap<String, Date> priorFirstAdded = outcome.getFirstAdded();
			HashMap<String, Date> updatedFirstAdded = new HashMap<>();

			for (String aui : outcome.getFirstAdded().keySet()) {
				for (Atom code : outcome.getCodes()) {
					if (aui.contentEquals(code.getAui()))
						updatedFirstAdded.put(aui, priorFirstAdded.get(aui));
				}
			}

			// Update code add date
			outcome.setFirstAdded(updatedFirstAdded);
		}
		return;
	}

	/**
	 * Bi-directional test to compare RxNorm ingredient list between this SplDrug
	 * entry and another. If all are found exact in both, returns true, otherwise
	 * false
	 * 
	 * @param p
	 * @return
	 */
	public boolean hasExactRxNormIngredients(SplDrug p) {
		// Must hvae ingredients to compare
		if (this.getRxNormPts().size() == 0 || p.getRxNormPts().size() == 0)
			return false;

		// Compare first all ingredients in this product
		for (String aui1 : this.rxNormPts.keySet()) {
			Atom rx = this.rxNormPts.get(aui1);
			if (rx != null) {
				if (rx.getTty().contentEquals("IN") || rx.getTty().contentEquals("PIN")) {
					boolean found = false;
					for (String aui2 : p.rxNormPts.keySet()) {
						if (aui1.contentEquals(aui2)) {
							found = true;
							break;
						}
					}
					if (found == false)
						return false;
				}
			}
		}

		// Next, compare all ingredients in product we are testing against
		for (String aui1 : p.rxNormPts.keySet()) {
			Atom rx = p.rxNormPts.get(aui1);
			if (rx != null) {
				if (rx.getTty().contentEquals("IN") || rx.getTty().contentEquals("PIN")) {
					boolean found = false;
					for (String aui2 : this.rxNormPts.keySet()) {
						if (aui1.contentEquals(aui2)) {
							found = true;
							break;
						}
					}
					if (found == false)
						return false;
				}
			}
		}

		return true;
	}

	/**
	 * Merge products based on substance identity. Primary rule: identical ACTIVE
	 * ingredients (ignoring excipients). Secondary (existing) rules: exact DP CUI
	 * match, subset of DP CUIs, or shared NDA.
	 * 
	 * @param p the product to merge with
	 * @return true if merged, false otherwise
	 * 
	 */

	/**
	 * Merge products based on MTHSPL DrugProduct CUI, with a hard guard: if both
	 * sides expose non-empty SNOMED ingredient sets and they differ, do NOT merge
	 * (prevents mono vs co-pack bleed-through).
	 */
	public boolean mergeProductGroup(SplDrug p) {
		// 0) Do not merge across different source types
		if (!hasSameSourceTypes(p))
			return false;

		// Never merge co-pack with non co-pack
		if (this.isCoPack() != p.isCoPack())
			return false;

		// --- NEW: Active-ingredient (SNOMED) guard ---
		// Use SNOMED ingredient AUIs as our "active moiety" identity.
		java.util.Set<String> myActives = new java.util.HashSet<>(this.getIngredients().keySet());
		java.util.Set<String> otherActives = new java.util.HashSet<>(p.getIngredients().keySet());

		boolean bothHaveActives = !myActives.isEmpty() && !otherActives.isEmpty();
		if (bothHaveActives && !myActives.equals(otherActives)) {
			// e.g., ribociclib vs ribociclib+letrozole -> don't merge
			return false;
		}

		// Optional: don’t allow BPCK-only to merge with non-BPCK when actives
		// differ/missing
		boolean thisHasBpck = this.getRxNormPts().values().stream().anyMatch(a -> "BPCK".equals(a.getTty()));
		boolean thatHasBpck = p.getRxNormPts().values().stream().anyMatch(a -> "BPCK".equals(a.getTty()));
		if (thisHasBpck ^ thatHasBpck) {
			// If one side is co-packish and the other isn’t, require exact actives if we
			// have them;
			// if we *don’t* have actives on both sides, be conservative and refuse to
			// merge.
			if (!bothHaveActives || !myActives.equals(otherActives))
				return false;
		}

		// 1) Existing equivalence triggers (tightened slightly)
		boolean dpOk = this.exactDrugProductMatch(p) || this.containsDrugProductCuis(p.getDrugProductCuis());
		boolean ndaOk = this.containsNda(p.getNdaIds());

		if (!(dpOk || ndaOk))
			return false;

		// If the only reason to merge is NDA overlap, insist on equal actives when
		// available
		if (!dpOk && bothHaveActives && !myActives.equals(otherActives))
			return false;

		// --- Perform the merge (unchanged logic, but now safe) ---

		// MTHSPL drug products
		this.getDrugProduct().putAll(p.getDrugProduct());

		// RxNorm
		this.getRxNormPts().putAll(p.getRxNormPts());

		// SNOMED (PTs + ingredients)
		this.getSnomedPts().putAll(p.getSnomedPts());
		this.getIngredients().putAll(p.getIngredients());

		// ATC
		this.getAtcCodes().putAll(p.getAtcCodes());

		// SNOMED parents
		for (String aui : p.getSnomedParentAuis())
			if (!this.getSnomedParentAuis().contains(aui))
				this.getSnomedParentAuis().add(aui);

		// DrugProduct CUIs
		for (String cui : p.getDrugProductCuis())
			if (!this.getDrugProductCuis().contains(cui))
				this.getDrugProductCuis().add(cui);

		// Merge the XML files and processing status of each if the same GUID is found
		if (p.getGuid().contentEquals(this.getGuid())) {
			for (String xml : p.getXmlFiles().keySet()) {
				boolean proc = p.getXmlFiles().get(xml);
				if (this.getXmlFiles().containsKey(xml) == false) {
					this.getXmlFiles().put(xml, proc);
				}
			}
		}

		// Merge GUID/XML + per-GUID metadata
		// Process any other GUID maps
		for (String guid : p.getMergedGuidXmlPairs().keySet()) {
			java.util.List<String> xmls = p.getMergedGuidXmlPairs().get(guid);
			this.getMergedGuidXmlPairs().merge(guid,
					(xmls == null) ? new java.util.ArrayList<>() : new java.util.ArrayList<>(xmls),
					(oldList, newList) -> {
						java.util.LinkedHashSet<String> set = new java.util.LinkedHashSet<>(oldList);
						set.addAll(newList);
						return new java.util.ArrayList<>(set);
					});
			if (p.getGuidApprovalDate().containsKey(guid))
				this.getGuidApprovalDate().put(guid, p.getGuidApprovalDate().get(guid));
			if (p.getGuidNda().containsKey(guid))
				this.getGuidNda().put(guid, p.getGuidNda().get(guid));
		}

		this.getMergedGuidXmlPairs().merge(p.getGuid(), new java.util.ArrayList<>(p.getXmlFilesAsList()),
				(oldList, newList) -> {
					java.util.LinkedHashSet<String> set = new java.util.LinkedHashSet<>(oldList);
					set.addAll(newList);
					return new java.util.ArrayList<>(set);
				});

		if (p.getGuidApprovalDate().containsKey(p.getGuid()))
			this.getGuidApprovalDate().put(p.getGuid(), p.getGuidApprovalDate().get(p.getGuid()));

		if (p.getGuidNda().containsKey(p.getGuid()))
			this.getGuidNda().put(p.getGuid(), p.getGuidNda().get(p.getGuid()));

		// Outcomes
		this.updateLabels(p);
		this.reconcileLabelFirstAddedDatesWithinDrug();

		return true;
	}

	/**
	 * Support to merge XML files in the UMLS loading process
	 * 
	 * @param p
	 */
	public void mergeUnmappedSpl(SplDrug p) {
		for (String xmlFile : p.getXmlFilesAsList()) {
			this.xmlFiles.put(xmlFile, false);
		}
		return;
	}

	/**
	 * Test if we contain the same NDA code
	 * 
	 * @param ndaIds
	 * @return
	 */
	private boolean containsNda(List<Integer> ndaIds) {

		List<Integer> myNdaIds = this.getNdaIds();
		for (int ndaId : ndaIds) {
			if (myNdaIds.contains(ndaId))
				return true;
		}
		return false;
	}

	/**
	 * Test that all DP cuis are found in our product
	 * 
	 * @param cuiSet
	 * @return
	 */
	private boolean containsDrugProductCuis(List<String> cuiSet) {
		Set<String> drugProductCuisSet = new HashSet<>(this.getDrugProductCuis());
		for (String cui : cuiSet) {
			if (!drugProductCuisSet.contains(cui)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Test that two SPL labels have exact matching DP CUIs
	 * 
	 * @param p
	 * @return
	 */
	private boolean exactDrugProductMatch(SplDrug p) {

		// If the products belong to the same SNOMED Parent, we will consider them the
		// same class
		if (this.getSnomedParentAuis().size() == 1 && p.getSnomedParentAuis().size() == 1) {
			if (this.getSnomedParentAuis().get(0).contentEquals(p.getSnomedParentAuis().get(0)))
				return true;
		}

		// Bi-directional test for CUI equivalence
		for (String cui : this.getDrugProductCuis()) {
			if (p.getDrugProductCuis().contains(cui) == false)
				return false;
		}

		for (String cui : p.getDrugProductCuis()) {
			if (this.getDrugProductCuis().contains(cui) == false)
				return false;
		}

		return true;
	}

	/**
	 * Get complete set of GUID-XML maps for this substance
	 * 
	 * @return
	 */
	public HashMap<String, List<String>> getGuidXmlMaps() {

		HashMap<String, List<String>> maps = new HashMap<String, List<String>>();
		maps.put(this.getGuid(), this.getXmlFilesAsList());

		// Get merged source files
		for (String guid : this.mergedGuidXmlPairs.keySet()) {
			maps.put(guid, this.mergedGuidXmlPairs.get(guid));
		}

		return maps;
	}

	/**
	 * Test if this substance contains at least one of the NDC codes found in the
	 * SPL label.
	 * 
	 * @param ndcCodes
	 * @return
	 */
	public boolean hasNdcCodes(List<String> ndcCodes) {
		for (String ndc : ndcCodes) {
			for (String aui : this.drugProduct.keySet()) {
				Atom dp = this.drugProduct.get(aui);
				if (dp.getCode().contentEquals(ndc) == true) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Get the list of non-normalized NDC codes associated with the substance
	 * 
	 * @return
	 */
	public List<String> getNdcCodes() {
		List<String> ndcs = new ArrayList<>();
		for (String aui : this.drugProduct.keySet()) {
			Atom dp = this.drugProduct.get(aui);
			String ndcCode = dp.getCode();
			if (StringUtils.isNoneEmpty(ndcCode)) {
				ndcs.add(ndcCode);
			}
		}
		return ndcs;
	}

	/**
	 * Get the list of normalized NDC codes associated with the substance
	 * 
	 * @return
	 */
	public List<String> getNormalizedNdcCodes() {
		List<String> ndcs = new ArrayList<>();
		for (String aui : this.drugProduct.keySet()) {
			Atom dp = this.drugProduct.get(aui);
			String ndcCode = dp.getCode();
			if (StringUtils.isNoneEmpty(ndcCode)) {
				String normNdc = normalizeNdc(ndcCode);
				if (StringUtils.isNoneEmpty(normNdc))
					ndcs.add(normNdc);
			}
		}
		return ndcs;
	}

	/**
	 * Create a copy of the SPL drug mappings
	 * 
	 * @return
	 */
	public SplDrug copySplDrug() {
		SplDrug copy = new SplDrug();
		copy.setId(-1);

		// Source type
		copy.setSourceType(this.getSourceType());
		copy.setGuid(this.getGuid());

		// Deep-copy maps/lists so the clone is independent
		copy.setDrugProduct(new HashMap<>(this.getDrugProduct()));
		copy.setDrugProductCuis(new ArrayList<>(this.getDrugProductCuis()));
		copy.setSnomedPts(new HashMap<>(this.getSnomedPts()));
		copy.setSnomedParentAuis(new ArrayList<>(this.getSnomedParentAuis()));
		copy.setRxNormPts(new HashMap<>(this.getRxNormPts()));
		copy.setIngredients(new HashMap<>(this.getIngredients()));

		// carry over xmls (deep copy via setter)
		copy.setXmlFiles(this.getXmlFiles());

		// if you want these carried too, copy defensively:
		copy.setGuidApprovalDate(new HashMap<>(this.getGuidApprovalDate()));
		copy.setGuidNda(new HashMap<>(this.getGuidNda()));
		copy.setMergedGuidXmlPairs(new HashMap<>(this.getMergedGuidXmlPairs()));
		copy.setAtcCodes(new HashMap<>(this.getAtcCodes()));
		copy.setSrlcs(new HashMap<>(this.getSrlcs()));

		return copy;
	}

	/**
	 * If the product being compared has all of it's SNOMED parent AUIs captured by
	 * this one, then merge
	 * 
	 * @param p
	 * @return
	 */
	public boolean hasSnomedParents(SplDrug p) {

		List<String> myParentAuis = this.getSnomedParentAuis();
		List<String> testParentAuis = p.getSnomedParentAuis();

		// Cannot match if there are no parents to match on
		if (myParentAuis.size() == 0 || testParentAuis.size() == 0)
			return false;

		// Compare all the parents of the product in question
		for (String aui : testParentAuis) {
			if (myParentAuis.contains(aui) == false) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Test if the product has any data prior to oldest start date
	 * 
	 * @param myOldestDate
	 * @return
	 */
	public boolean hasDataPrior(Date myOldestDate) {

		for (String aui : this.getExactMatchIndications().getFirstAdded().keySet()) {
			Date indDte = this.getExactMatchIndications().getFirstAdded().get(aui);
			if (indDte.before(myOldestDate))
				return true;
		}

		for (String aui : this.getExactMatchWarnings().getFirstAdded().keySet()) {
			Date indDte = this.getExactMatchWarnings().getFirstAdded().get(aui);
			if (indDte.before(myOldestDate))
				return true;
		}

		for (String aui : this.getExactMatchBlackbox().getFirstAdded().keySet()) {
			Date indDte = this.getExactMatchBlackbox().getFirstAdded().get(aui);
			if (indDte.before(myOldestDate))
				return true;
		}

		return false;
	}

	/**
	 * Test for any labels added between the observation window
	 * 
	 * @param windowStart
	 * @param windowEnd
	 * @return
	 */
	public boolean hasNewLabel(Date windowStart, Date windowEnd) {

		for (String aui : this.getExactMatchWarnings().getFirstAdded().keySet()) {
			Date aeDte = this.getExactMatchWarnings().getFirstAdded().get(aui);
			if (aeDte.before(windowEnd) && aeDte.after(windowStart))
				return true;
		}

		for (String aui : this.getExactMatchBlackbox().getFirstAdded().keySet()) {
			Date aeDte = this.getExactMatchBlackbox().getFirstAdded().get(aui);
			if (aeDte.before(windowEnd) && aeDte.after(windowStart))
				return true;
		}

		return false;
	}

	private List<Atom> getLabelEvent(Outcome outcome, Date windowStart, Date windowEnd, Outcome ignoreOutcome) {
		List<Atom> results = new ArrayList<>();
		HashMap<String, Boolean> ignorePriors = new HashMap<>();

		if (ignoreOutcome != null) {
			for (String aui : ignoreOutcome.getFirstAdded().keySet()) {
				Date aeDte = ignoreOutcome.getFirstAdded().get(aui);
				if (aeDte.before(windowStart)) {
					ignorePriors.put(aui, true);
				}
			}
		}

		for (String aui : outcome.getFirstAdded().keySet()) {
			if (!ignorePriors.containsKey(aui)) {
				Date aeDte = outcome.getFirstAdded().get(aui);
				if (aeDte.before(windowEnd) && aeDte.after(windowStart)) {
					for (Atom atom : outcome.getCodes()) {
						if (atom.getAui().contentEquals(aui)) {
							results.add(atom);
						}
					}
				}
			}
		}

		return results;
	}

	/**
	 * Get meddra entries that match window
	 * 
	 * @param windowStart
	 * @param windowEnd
	 * @return
	 */
	public List<Atom> getWarningLabelEvent(Date windowStart, Date windowEnd) {
		return getLabelEvent(this.getExactMatchWarnings(), windowStart, windowEnd, this.getExactMatchBlackbox());
	}

	/**
	 * Get meddra entries that match window
	 * 
	 * @param windowStart
	 * @param windowEnd
	 * @return
	 */
	public List<Atom> getBlackboxLabelEvent(Date windowStart, Date windowEnd) {
		return getLabelEvent(this.getExactMatchBlackbox(), windowStart, windowEnd, this.getExactMatchWarnings());
	}

	public void setNda(int ndaId) {
		if (ndaId > 0) {
			this.guidNda.put(this.getGuid(), ndaId);
		}
	}

	/**
	 * Get unique set of NDA ids associated with this substance in the database.
	 * 
	 * @return
	 */
	public List<Integer> getNdaIds() {
		List<Integer> ndas = new ArrayList<>();
		for (String guid : this.getGuidNda().keySet())
			ndas.add(this.getGuidNda().get(guid));
		return ndas;
	}

	/**
	 * Add associated SRLCs
	 * 
	 * @param s
	 */
	public void addSrlc(Srlc s) {
		this.getSrlcs().put(s.getDrugId(), s);
		return;
	}

	/**
	 * Update the labels found from the SRLC updates
	 * 
	 * @param srlc
	 */
	public void updateLabels(Srlc srlc) {

		// Update product outcome dates
		updateOutcome(this.exactMatchWarnings, srlc.getExactAeMatch(), true);
		updateOutcome(this.exactMatchBlackbox, srlc.getExactBlackboxMatch(), true);
		updateOutcome(this.nlpMatchWarnings, srlc.getNlpAeMatch(), true);
		updateOutcome(this.nlpMatchBlackbox, srlc.getNlpBlackboxMatch(), true);

		// Rerun resolution
		this.resolveLabeledEvents();

	}

	/**
	 * Return the NDA linked directly to this SPL GUID
	 * 
	 * @return
	 */
	public int getPrimaryNda() {
		int ndaId = -1;
		if (this.getGuidNda().containsKey(this.getGuid())) {
			ndaId = this.getGuidNda().get(this.getGuid());
		}
		return ndaId;
	}

	/**
	 * Update the approval date with the full date from FDA data if no GUID approval
	 * date is assigned
	 * 
	 * @param dte
	 */
	public void updateGuidDates(Date dte) {
		boolean updated = false;
		for (String guid : this.getGuidApprovalDate().keySet()) {
			Date compDte = this.getGuidApprovalDate().get(guid);
			if (compDte == null && dte != null) {
				// Update with full approval date
				this.getGuidApprovalDate().put(guid, dte);
				updated = true;
			}
		}

		if (updated == false) {
			// update all GUIDs with the current approval date
			for (String guid : this.getGuidXmlMaps().keySet()) {
				if (this.getGuidApprovalDate().containsKey(guid) == false) {
					this.getGuidApprovalDate().put(guid, dte);
				}
			}
		}
	}

	/**
	 * Reconcile "firstAdded" dates within THIS SplDrug. For each CUI, compute the
	 * earliest label date seen in the relevant buckets and update all matching AUIs
	 * in those buckets to that minimum.
	 *
	 * Policy: - Indications: min across (exactMatchIndications +
	 * nlpMatchIndications) - Warnings/BlackBox: min across (exactMatchWarnings +
	 * nlpMatchWarnings + exactMatchBlackbox + nlpMatchBlackbox)
	 *
	 * Null dates are ignored. If no date exists for a CUI anywhere, nothing is
	 * changed.
	 */
	public void reconcileLabelFirstAddedDatesWithinDrug() {

		// --- helpers ---
		class Bucket {
			final java.util.Collection<Atom> codes;
			final java.util.Map<String, java.util.Date> firstAddedByAui;

			Bucket(java.util.Collection<Atom> codes, java.util.Map<String, java.util.Date> map) {
				this.codes = codes;
				this.firstAddedByAui = map;
			}
		}

		java.util.function.BiConsumer<java.util.Map<String, java.util.Date>, Bucket> foldMinByCui = (minByCui,
				bucket) -> {
			if (bucket == null || bucket.codes == null || bucket.firstAddedByAui == null)
				return;
			for (Atom a : bucket.codes) {
				if (a == null)
					continue;
				java.util.Date d = bucket.firstAddedByAui.get(a.getAui());
				if (d == null)
					continue;
				minByCui.merge(a.getCui(), d, (oldD, newD) -> (newD.before(oldD) ? newD : oldD));
			}
		};

		java.util.function.BiConsumer<java.util.Map<String, java.util.Date>, Bucket> pushMinIntoBucket = (minByCui,
				bucket) -> {
			if (bucket == null || bucket.codes == null || bucket.firstAddedByAui == null)
				return;
			for (Atom a : bucket.codes) {
				java.util.Date min = minByCui.get(a.getCui());
				if (min == null)
					continue;
				java.util.Date curr = bucket.firstAddedByAui.get(a.getAui());
				if (curr == null || min.before(curr)) {
					bucket.firstAddedByAui.put(a.getAui(), min);
				}
			}
		};

		// --- build buckets from this SplDrug ---
		Bucket exactInd = new Bucket(getExactMatchIndications().getCodes(), getExactMatchIndications().getFirstAdded());
		Bucket nlpInd = new Bucket(getNlpMatchIndications().getCodes(), getNlpMatchIndications().getFirstAdded());

		Bucket exactWarn = new Bucket(getExactMatchWarnings().getCodes(), getExactMatchWarnings().getFirstAdded());
		Bucket nlpWarn = new Bucket(getNlpMatchWarnings().getCodes(), getNlpMatchWarnings().getFirstAdded());
		Bucket exactBB = new Bucket(getExactMatchBlackbox().getCodes(), getExactMatchBlackbox().getFirstAdded());
		Bucket nlpBB = new Bucket(getNlpMatchBlackbox().getCodes(), getNlpMatchBlackbox().getFirstAdded());

		// --- 1) Indications: compute min per CUI, then push back into both Indication
		// buckets ---
		java.util.Map<String, java.util.Date> minIndByCui = new java.util.HashMap<>(256);
		foldMinByCui.accept(minIndByCui, exactInd);
		foldMinByCui.accept(minIndByCui, nlpInd);

		pushMinIntoBucket.accept(minIndByCui, exactInd);
		pushMinIntoBucket.accept(minIndByCui, nlpInd);

		// --- 2) Warnings/BlackBox: compute min per CUI across all warning-like
		// buckets, then push back ---
		java.util.Map<String, java.util.Date> minWarnByCui = new java.util.HashMap<>(256);
		foldMinByCui.accept(minWarnByCui, exactWarn);
		foldMinByCui.accept(minWarnByCui, nlpWarn);
		foldMinByCui.accept(minWarnByCui, exactBB);
		foldMinByCui.accept(minWarnByCui, nlpBB);

		pushMinIntoBucket.accept(minWarnByCui, exactWarn);
		pushMinIntoBucket.accept(minWarnByCui, nlpWarn);
		pushMinIntoBucket.accept(minWarnByCui, exactBB);
		pushMinIntoBucket.accept(minWarnByCui, nlpBB);
	}

	

	/**
	 * Get primary CUIS for this product/substance
	 * 
	 * @return
	 */
	public List<String> getPrimaryCuis() {

		List<String> cuis = new ArrayList<>();
		for (String cui : this.getDrugProductCuis()) {
			if (cuis.contains(cui) == false)
				cuis.add(cui);
		}

		for (String aui : this.getRxNormPts().keySet()) {
			Atom rxNorm = this.getRxNormPts().get(aui);
			String cui = rxNorm.getCui();
			if (rxNorm.getTty().contentEquals("SBD") || rxNorm.getTty().contentEquals("PSN")) {
				if (cuis.contains(cui) == false)
					cuis.add(cui);
			}
		}

		return cuis;
	}

	/**
	 * This follows the same logic as the RxNorm normalization method, minus the
	 * final two digits
	 * 
	 * See: Sec 6.0 Normalizing NDC codes in RxNorm
	 * https://www.nlm.nih.gov/research/umls/rxnorm/docs/techdoc.html
	 * 
	 * @param sourceNdc
	 * @return
	 */
	public static String normalizeNdc(String sourceNdc) {
		try {
			String normalizedNdc = "";

			// Split the source NDC code by the dash
			String[] parts = sourceNdc.split("-");

			// Handle 4-4 format
			if (parts.length == 2 && parts[0].length() == 4 && parts[1].length() == 4) {
				// Prefix first part with a leading zero to make it 5 digits
				String part1 = "0" + parts[0];
				normalizedNdc = part1 + parts[1];
			}
			// Handle 5-4 format
			else if (parts.length == 2 && parts[0].length() == 5 && parts[1].length() == 4) {
				normalizedNdc = parts[0] + parts[1];
			}

			return normalizedNdc;
		} catch (Exception e) {
			return "";
		}
	}

	public List<Atom> getAtcClasses() {
		List<Atom> atcs = new ArrayList<>();
		for (String ndc : this.atcCodes.keySet()) {
			Atom atc = this.atcCodes.get(ndc);
			atcs.add(atc);
		}
		return atcs;
	}

	/**
	 * Test that both products contain the exact same ATC classes
	 * 
	 * @param prd2
	 * @return
	 */
	public boolean hasExactAtcClass(SplDrug prd2) {

		List<Atom> myAtcs = this.getAtcClasses();
		List<Atom> compAtcs = prd2.getAtcClasses();

		// If either contain no ATC classes, then the comparison fails
		if (myAtcs.size() == 0 || compAtcs.size() == 0)
			return false;

		// Test both directions
		for (Atom a : myAtcs) {
			if (compAtcs.contains(a) == false)
				return false;
		}

		for (Atom a : compAtcs) {
			if (myAtcs.contains(a) == false)
				return false;
		}

		return true;
	}

	/**
	 * Return list of CUIs found in the indications for this GUID
	 * 
	 * @param umls
	 * @return
	 */
	public List<String> getAllIndCuis() {
		List<String> cuiSet = new ArrayList<>();

		// All of our outcomes
		Outcome[] outcomes = new Outcome[] { exactMatchIndications, nlpMatchIndications };
		for (Outcome outcome : outcomes) {
			if (outcome != null) {
				for (Atom mdr : outcome.getCodes()) {
					String cui = mdr.getCui();
					if (cuiSet.contains(cui) == false) {
						cuiSet.add(cui);
					}
				}
			}
		}

		return cuiSet;
	}

	/**
	 * Return list of CUIs found in the warnings for this product
	 * 
	 * @param umls
	 * @return
	 */
	public List<String> getAllAeCuis() {
		List<String> cuiSet = new ArrayList<>();

		// All of our outcomes
		Outcome[] outcomes = new Outcome[] { exactMatchWarnings, exactMatchBlackbox, nlpMatchWarnings,
				nlpMatchBlackbox };
		for (Outcome outcome : outcomes) {
			if (outcome != null) {
				for (Atom mdr : outcome.getCodes()) {
					String cui = mdr.getCui();
					if (cuiSet.contains(cui) == false) {
						cuiSet.add(cui);
					}
				}
			}
		}

		return cuiSet;
	}

	/**
	 * Return list of CUIs found in the warnings for this GUID
	 * 
	 * @param umls
	 * @param guid
	 * @return
	 */
	public List<String> getAeCuis(UmlsLoader umls, String guid) {
		List<String> cuiSet = new ArrayList<>();

		// All of our outcomes
		Outcome[] outcomes = new Outcome[] { exactMatchWarnings, exactMatchBlackbox, nlpMatchWarnings,
				nlpMatchBlackbox };
		for (Outcome outcome : outcomes) {
			if (outcome != null) {
				if (outcome.getOutcomeSource().containsKey(guid)) {
					for (String aui : outcome.getOutcomeSource().get(guid)) {
						// Get the MedDRA term
						Atom mdr = umls.getMedDRA().get(aui);
						String cui = mdr.getCui();
						if (cuiSet.contains(cui) == false) {
							cuiSet.add(cui);
						}
					}
				}
			}
		}

		return cuiSet;
	}

	/**
	 * Return list of CUIs found in the indications for this GUID
	 * 
	 * @param umls
	 * @param guid
	 * @return
	 */
	public List<String> getIndCuis(UmlsLoader umls, String guid) {
		List<String> cuiSet = new ArrayList<>();

		// All of our outcomes
		Outcome[] outcomes = new Outcome[] { exactMatchIndications, nlpMatchIndications };
		for (Outcome outcome : outcomes) {
			if (outcome != null) {
				if (outcome.getOutcomeSource().containsKey(guid)) {
					for (String aui : outcome.getOutcomeSource().get(guid)) {
						// Get the MedDRA term
						Atom mdr = umls.getMedDRA().get(aui);
						String cui = mdr.getCui();
						if (cuiSet.contains(cui) == false) {
							cuiSet.add(cui);
						}
					}
				}
			}
		}

		return cuiSet;
	}

	public HashMap<String, Boolean> getXmlFiles() {
		return this.xmlFiles;
	}

	public List<String> getXmlFilesAsList() {
		List<String> files = new ArrayList<>();
		for (String xml : this.xmlFiles.keySet()) {
			files.add(xml);
		}
		return files;
	}

	// 3) Defensive setter (deep copy; accepts null)
	public void setXmlFiles(HashMap<String, Boolean> files) {
		this.xmlFiles = (files == null) ? new HashMap<>() : new HashMap<>(files);
	}

	// Convenience helper
	public void addXmlFile(String xml, Boolean proc) {
		getXmlFiles().put(xml, proc);
	}

	// --- Active-ingredient helpers ----------------------------------------------

	/** Active CUIs from SNOMED ingredient map (usually just actives). */
	private java.util.Set<String> activeSnomedCuis() {
		java.util.Set<String> s = new java.util.HashSet<>();
		if (this.ingredients != null) {
			for (Atom ing : this.ingredients.values()) {
				if (ing != null && ing.getCui() != null && !ing.getCui().isBlank()) {
					s.add(ing.getCui());
				}
			}
		}
		return s;
	}

	/** RxNorm IN/PIN CUIs that correspond to our SNOMED active CUIs. */
	private java.util.Set<String> rxnormActiveIngredientCuis() {
		java.util.Set<String> actives = activeSnomedCuis();
		if (actives.isEmpty())
			return java.util.Collections.emptySet();

		java.util.Set<String> out = new java.util.HashSet<>();
		if (this.rxNormPts != null) {
			for (Atom rx : this.rxNormPts.values()) {
				if (rx == null)
					continue;
				String tty = rx.getTty();
				String cui = rx.getCui();
				if (cui != null && actives.contains(cui) && ("IN".equals(tty) || "PIN".equals(tty))) {
					out.add(cui);
				}
			}
		}
		return out;
	}

	/**
	 * Optional: a small signature to keep formulation differences (excipients)
	 * visible.
	 */
	public String excipientSignature() {
		// All IN/PIN CUIs minus actives -> sorted -> joined
		java.util.Set<String> active = rxnormActiveIngredientCuis();
		java.util.TreeSet<String> excipients = new java.util.TreeSet<>();
		if (this.rxNormPts != null) {
			for (Atom rx : this.rxNormPts.values()) {
				if (rx == null)
					continue;
				String tty = rx.getTty();
				String cui = rx.getCui();
				if (cui != null && ("IN".equals(tty) || "PIN".equals(tty)) && !active.contains(cui)) {
					excipients.add(cui);
				}
			}
		}
		return String.join("|", excipients);
	}

	// In SplDrug.java
	public boolean isCoPack() {
		for (Atom a : this.getRxNormPts().values()) {
			if ("BPCK".equals(a.getTty()))
				return true;
		}
		return false;
	}

}
