import hail as hl
import os, sys

config = {'spark.driver.memory' : '10g', 'spark.executor.memory' : '10g'}
hl.init(quiet = True, spark_conf = config, skip_logging_configuration = True)

def Sample_QC(mt, sample_info_path):
    
    print("[Start Sample QC]")
        
    #Read sample info
    types = {'RemoveOrNot': hl.tbool}
    sample_info = hl.import_table(sample_info_path, key = 'vcfID', types=types)
    sample_info.describe()
    
    #Annotate sample info
    mt = mt.annotate_cols(sample_info = sample_info[mt.s])
    
    #Remove samples
    mt = mt.filter_cols(mt.sample_info.RemoveOrNot==False) 
    print("--> After sample(peddy) QC, sample size is reduced to :", mt.cols().count())
           
    return mt

def Variant_QC(mt):
    print("[Start Variant QC]")
    
    print("--> After remove variants with VSQR pass failure, in LCR, or with multi allele, the number of variants is :", mt.rows().count())
    
    #Genotype QC
    print("--> Before genotype QC, the number of entries is :", mt.entries().count())
    mt = mt.filter_entries((mt.DP >=10) & (mt.DP <= 1000))
    mt = annotateAB(mt)
    filter_condition = ((mt.GT.is_hom_ref() & (mt.AB <= 0.1) & (mt.GQ >= 25)) |
                        (mt.GT.is_het() & (mt.AB >= 0.25) & (mt.PL[0] >= 25)) |
                        (mt.GT.is_hom_var() & (mt.AB >= 0.9) & (mt.PL[0] >= 25)))
    mt = mt.filter_entries(filter_condition)
    mt = mt.transmute_entries(GT = hl.if_else(((mt.sample_info.sex=='F') & (mt.locus.contig=="chrY")), hl.null('call'), mt.GT))
    mt = mt.transmute_entries(GT = hl.if_else(((mt.sample_info.sex=='M') & (mt.locus.contig=="chrX") & (mt.GT.is_het())), hl.null('call'), mt.GT))
    print("--> After genotype QC, the number of entries is :", mt.entries().count())
    
    #Variant QC
    mt = hl.variant_qc(mt)
    mt = mt.filter_rows((mt.variant_qc.call_rate >=0.1) & (mt.variant_qc.p_value_hwe >1e-12))
    print("--> After variant QC(call rate and HWE test), the number of variants is reduced to :", mt.rows().count())
   
    return mt


def Find_DNV(mt, fam_path, non_neuro_gnomad_path):
    
    print("[De novo workflow starts.]")
    
    #Read prior AF hail table and annotate AF
    non_neuro_gnomad = hl.read_table(non_neuro_gnomad_path)  
    mt = mt.annotate_rows(non_neuro_gnomad_af = non_neuro_gnomad[mt.locus, mt.alleles].AF)
    
    #DNV filtering
    trio_fam = hl.Pedigree.read(fam_path)
    dnv_test = hl.de_novo(mt=mt, 
                           pedigree=trio_fam,
                           pop_frequency_prior=mt.non_neuro_gnomad_af,  
                           max_parent_ab=0.1,
                           min_child_ab = 0.3,
                           min_dp_ratio = 0.3,
                           min_gq=25)
    
    print("--> De novo variants from complete trio(n_proband = 239) : ", dnv_test.aggregate(hl.agg.counter(dnv_test.confidence)))
    print("--> Only variants with High/Medium confidence are used.")
    
    dnv_test = dnv_test.filter((dnv_test.confidence=="HIGH") | (dnv_test.confidence=="MEDIUM"))
    return dnv_test

def Find_rare_het(mt, internal_AC=1):
    
    print("[Rare heterozygous variants workflow starts.]")
    
    filter_condition_entry = ((mt.GT.is_het()) & (mt.GQ >= 25) & (mt.AB >= 0.3)) 
    mt = mt.filter_entries(filter_condition_entry)
    print("--> After remove variants with low quality(based on GQ, AB), the number of het entries is :", mt.entries().count())
    filter_condition_variant = (((hl.is_snp(mt.alleles[0], mt.alleles[1])) & (mt.info.VQSLOD >= -2.085))|
                                (~hl.is_snp(mt.alleles[0], mt.alleles[1])))    
    mt = mt.filter_rows(filter_condition_variant)
    print("--> After remove SNVs with VQSLOD<-2.085, the number of variants is reduced to :", mt.rows().count())
     
    #AC <=1(cf. satterstrom 2020 AC ≤ 5 in ASC(18,153))
    mt = mt.filter_rows(mt.variant_qc.AC[1]<=internal_AC)
    
    #Select variants from proband
    mt = mt.filter_cols(mt.sample_info.ROLE=='p')
    tb = mt.entries()
    tb = tb.key_by(tb.locus, tb.alleles, tb.s)
    print("--> In 537(239(trio)+298(singleton)) NDD patients, there are ", tb.count(), " hight quality rare het mutations.")
    
    return tb

def Merge_DNV_and_Rare_het(DNV, Rare_het, mt):
    
    print("[Merge Two tables(DNV and Rare het)]")
    
    #Annotate Type and remove intersect variant from rare-het variants list
    DNV = DNV.annotate(InheritanceType = "DNV")
    Rare_het = Rare_het.annotate(InheritanceType = "RareHet")
    
    tmp = DNV.semi_join(Rare_het)
    print("--> Remove intersect variants(DNV & RareHet) from rare-het variants list : -", tmp.count())
    Rare_het = Rare_het.anti_join(DNV)
    
    
    #Union(DNV + rare inherited)
    DNV = DNV.select(DNV.InheritanceType); DNV = DNV.rename({'id':'s'})
    Rare_het = Rare_het.select(Rare_het.InheritanceType)
    DNV_RareHet = DNV.union(Rare_het)
    
    #Make Matrix Table Table
    tb = mt.entries()
    tb = tb.key_by(tb.locus, tb.alleles, tb.s)
    tb = tb.annotate(InheritanceType = DNV_RareHet[tb.locus, tb.alleles, tb.s].InheritanceType)
    tb = tb.filter(~hl.is_missing(tb.InheritanceType))
    print("--> Variants passed filter of sequencing-based quality metrics :", tb.aggregate(hl.agg.counter(tb.InheritanceType)))
    
    return tb


def Classify_variants(tb, MPC_score_path):
    
    print("[Classify variants into synonymous, benign missense, damaging missense and PTVs.]")
    
    #Read MPC table and annotate MPC score
    MPC = hl.read_table(MPC_score_path)
    MPC = MPC.key_by("locus", "alleles", "gene_name")
    
    tb = tb.annotate(gene_symbol = tb.vep.transcript_consequences.gene_symbol[0])
    tb = tb.key_by(tb.locus, tb.alleles, tb.gene_symbol)
    tb = tb.annotate(MPC = MPC[tb.locus, tb.alleles, tb.gene_symbol].MPC)
    tb = tb.key_by(tb.locus, tb.alleles, tb.s)
    print("--> After MPC score annotation, the number of variants:", tb.count())

    
    #Classify variants into synonymous, benign missense, damaging missense and PTVs.    
    PTV = ((hl.array(['frameshift_variant', 'splice_acceptor_variant', 'splice_donor_variant', 'stop_gained']).contains(tb.vep.most_severe_consequence)) &
           (((tb.vep.transcript_consequences.lof[0]=='HC') & (hl.is_missing(tb.vep.transcript_consequences.lof_flags[0])))|
            ((tb.vep.transcript_consequences.lof[0]=='LC') & ((tb.vep.transcript_consequences.lof_flags[0]=='SINGLE_EXON') |(hl.is_missing(tb.vep.transcript_consequences.lof_flags[0]))))))
    damaging_missense = ((hl.array(['inframe_deletion', 'inframe_insertion', 'missense_variant', 'stop_lost', 'start_lost','protein_altering_variant']).contains(tb.vep.most_severe_consequence))&
                         (((tb.vep.transcript_consequences.polyphen_prediction[0] == 'probably_damaging') & (tb.vep.transcript_consequences.sift_prediction[0] =='deleterious'))|
                          (tb.vep.transcript_consequences.cadd_phred[0] > 20)|
                          (tb.MPC >= 2)))
    benign_missense = ((hl.array(['inframe_deletion', 'inframe_insertion', 'missense_variant', 'stop_lost', 'start_lost','protein_altering_variant']).contains(tb.vep.most_severe_consequence))&
                       (tb.vep.transcript_consequences.polyphen_prediction[0] == 'benign') & 
                       (tb.vep.transcript_consequences.sift_prediction[0] =='tolerated'))
    synonymous = hl.array(['synonymous_variant', 'stop_retained_variant','incomplete_terminal_codon_variant']).contains(tb.vep.most_severe_consequence)
        
    tb = tb.annotate(VariantType = hl.if_else(PTV, "protein_truncating", 
                                              hl.if_else(damaging_missense,"damaging_missense",
                                                         hl.if_else(benign_missense, "benign_missense",
                                                                    hl.if_else(synonymous, "Synonymous",hl.null('str'))))))
    
    print("--> After classifying variants:", tb.aggregate(hl.agg.counter(tb.VariantType)) )
    print("--> Remove variants with no class.")
    
    tb = tb.filter(hl.is_missing(tb.VariantType), keep=False)            
    return tb

def Categorize_allele_frequency(tb, AC_table_path):
    
    #Read AC table and annotate AC
    AC_table = hl.read_table(AC_table_path)
    tb = tb.key_by(tb.locus, tb.alleles)
    tb = tb.annotate(public_DB_AC = AC_table[tb.locus, tb.alleles])
    tb = tb.key_by(tb.locus, tb.alleles, tb.s)   

    #Keep only ultra-rare variants
    tb = tb.filter((hl.is_missing(tb.public_DB_AC.total_AC) | (tb.public_DB_AC.total_AC==0))) 
    print("--> The number of ultra-rare variants:", tb.aggregate(hl.agg.counter(tb.VariantType)) )
    
    return tb

def export_table(tb, tb_path):
    tb = tb.flatten()
    tb= tb.explode(tb['vep.transcript_consequences'])
    tb = tb.flatten()
    tb = tb.drop(tb['vep.input'])
    tb.export(tb_path)



def main():
    mt_after_QC = hl.read_matrix_table("/Users/lizzychoi/Projects/UDP_WES/Inputs/UDP_WES_after_QC_20210603.mt/")

    # [step2]de novo, rare heterozygous workflows
    internal_AC = int(sys.argv[1])
    DNVs = Find_DNV(mt_after_QC, "/Users/lizzychoi/Projects/UDP_WES/Documents/ArrangeSampleList/UDP_WES_after_peddy.fam", "/Users/lizzychoi/Resources/gnomad/gnomadv3.1_exon_non_neuro_adj.ht")
    Rare_het = Find_rare_het(mt_after_QC, internal_AC)
    DNV_RareHet = Merge_DNV_and_Rare_het(DNVs, Rare_het, mt_after_QC)

    # [step3]Classify Variants(& Remove the others)
    DNV_RareHet_classified = Classify_variants(DNV_RareHet, "/Users/lizzychoi/Resources/MPC/MPC38.ht")  
    DNV_RareHet_classified = DNV_RareHet_classified.checkpoint("/Users/lizzychoi/Projects/UDP_WES/Inputs/UDP_WES_checkpoint.mt/")

    # [step4]Select ultra-rare variants
    DNV_RareHet_classified_with_AC = Categorize_allele_frequency(DNV_RareHet_classified, "/Users/lizzychoi/Resources/201225_togovar_korea1k_gnomad_merged.ht/")
    export_table(DNV_RareHet_classified_with_AC, f"/Users/lizzychoi/Projects/UDP_WES/Inputs/UDP_WES_QV_AC_{str(internal_AC)}.tsv.bgz")
    return 0



if __name__=='__main__':
    main()








