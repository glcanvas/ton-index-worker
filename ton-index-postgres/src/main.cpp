#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/check.h"
#include "DbScanner.h"
#include "IndexScheduler.h"
#include "json/json.h"
#include <json/reader.h>
#include <json/writer.h>
#include <json/value.h>
//
#include "td/utils/logging.h"
#include "crypto/vm/cp0.h"
#include "InsertManagerPostgres.h"
#include "DataParser.h"
#include "EventProcessor.h"
#include <string>
#include <fstream>


int main(int argc, char *argv[]) {
    auto lizardMsg =
            "te6cckEBAQEAVAAAoyl0N88AAAAAAAAAACBNKAGjs890VALrhTX/Ewo2g8beexKdGSiQwuR4Gfqjs2R+7QA0dnnuioBdcKa/4mFG0Hjbz2JToyUSGFyPAz9UdmyP3ZDvhJYZ";
    auto lizardMsgCell = vm::std_boc_deserialize(td::base64_decode(td::Slice(lizardMsg)).move_as_ok()).move_as_ok();

    auto code =
            "te6ccgEBAQEAIwAIQgJjHPmLuz7lQNrMSCVpddjiwwgMY085AFzNjhWTyrADXw==";
    auto data =
            "te6ccgEBBAEAxQABJSBfXhACBfXhACBfXhAAAAAoABUBAsmAAXD8SCCQUV3BXpRwAq/6SzOJYLO7UdpUmFkn8H9kDYtQAWyY52oQTBmzFCcIQFy5/AYooHBlzCYUgtJewwYXWAAaAGjF6rPZvhh+QgqVqb8xLtcqRxONtCQqCN6spZgZ1RqgQAIDCEICBQ7SEH9zCkSTz83bxnvbg4VPGQWzSGI2Uw1aUHIdGBcIQgJ8HzqgEmVFMFiIc3ijWM83MUhWNQQAgEwWm2SMP9++TA==";

    auto lizardAddr = block::StdAddress(td::Slice("0QDOMvX7WVRVdHRHzsorwwZP5zs2UrMPvxJsqw0ko94jFXqX"));
    std::cout << lizardAddr.rserialize();


    auto code_cell = vm::std_boc_deserialize(td::base64_decode(td::Slice(code)).move_as_ok()).move_as_ok();
    auto data_cell = vm::std_boc_deserialize(td::base64_decode(td::Slice(data)).move_as_ok()).move_as_ok();
    ton::SmartContract smc({code_cell, data_cell});
    ton::SmartContract::Args args;

    vm::CellBuilder anycast_cb;
    anycast_cb.store_bool_bool(false);
    auto anycast_cell = anycast_cb.finalize();
    td::Ref<vm::CellSlice> anycast_cs = vm::load_cell_slice_ref(anycast_cell);

    vm::CellBuilder cb;
    block::gen::t_MsgAddressInt.pack_addr_std(cb, anycast_cs, lizardAddr.workchain, lizardAddr.addr);
    auto owner_address_cell = cb.finalize();

    //args.set_libraries(vm::Dictionary(blocks_ds.config_->get_libraries_root(), 256));
    //args.set_config(blocks_ds.config_);
    args.set_now(td::Time::now());
    args.set_address(
            block::StdAddress(td::Slice("kQD1nhJreWnpKLVPm2NhYa-Hmxc94gkBROaTWlw7oDT3JmV4"))
    );
    args.libraries
    args.set_stack({vm::StackEntry(vm::load_cell_slice_ref(owner_address_cell))});

    //args.set_method_id("get_wallet_address");
    args.vm_log_verbosity_level = 5;
    args.limits = vm::GasLimits{1000000, 1000000};
    // auto res = smc.send_internal_message(lizardMsgCell, args);
    // std::cout << res.code;

    // 75693 -- get owner address
    //args.set_method_id(75693); // 74945, 75693
    auto res = smc.send_external_message(lizardMsgCell, args);
    std::cout << res.code << std::endl;

    auto stack = res.stack->as_span();
    auto cnt = res.stack->depth();
    for (auto i = 0; i < cnt; i++) {
        std::cout << "!! here!!" << stack[i].type() << std::endl;
    }

}
//
//const block::StdAddress evaa_contract = block::StdAddress::parse(
//        "EQC8rUZqR_pWV1BylWUlPNBzyiTYVoBEmQkMIQDZXICfnuRr").move_as_ok();
//
//struct Tokens {
//    std::string name;
//    std::string code;
//    std::string data;
//    block::StdAddress address;
//};
//
//const Tokens master_contract_tokens[5] = {
//        Tokens{
//                "stTON",
//                "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",
//                "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",
//                block::StdAddress::parse("EQDNhy-nxYFgUqzfUzImBEP67JqsyMIcyk2S5_RwNNEYku0k").move_as_ok()
//        },
//        Tokens{
//                "USDT",
//                "te6ccgECGAEABbsAART/APSkE/S88sgLAQIBYgIDAgLLBAUCASAUFQLz0MtDTAwFxsI47MIAg1yHTHwGCEBeNRRm6kTDhgEDXIfoAMO1E0PoA+kD6QNTU0VBFoUE0yFAF+gJQA88WAc8WzMzJ7VTg+kD6QDH6ADH0AfoAMfoAATFw+DoC0x8BAdM/ARLtRND6APpA+kDU1NEmghBkK30HuuMCJoGBwAdojhkZYOA54tkgUGD+gvAAZY1NVFhxwXy4EkE+kAh+kQwwADy4U36ANTRINDTHwGCEBeNRRm68uBIgEDXIfoA+kAx+kAx+gAg1wsAmtdLwAEBwAGw8rGRMOJUQxsIA/qCEHvdl966juc2OAX6APpA+ChUEgpwVGAEExUDyMsDWPoCAc8WAc8WySHIywET9AAS9ADLAMn5AHB0yMsCygfL/8nQUAjHBfLgShKhRBRQZgPIUAX6AlADzxYBzxbMzMntVPpA0SDXCwHAALORW+MN4CaCECx2uXO64wI1JQoLDAGOIZFykXHi+DkgbpOBJCeRIOIhbpQxgShzkQHiUCOoE6BzgQOjcPg8oAJw+DYSoAFw+Dagc4EECYIQCWYBgHD4N6C88rAlWX8JAOyCEDuaygBw+wL4KEUEcFRgBBMVA8jLA1j6AgHPFgHPFskhyMsBE/QAEvQAywDJIPkAcHTIywLKB8v/ydDIgBgBywUBzxZY+gICmFh3UAPLa8zMlzABcVjLasziyYAR+wBQBaBDFMhQBfoCUAPPFgHPFszMye1UAETIgBABywUBzxZw+gJwActqghDVMnbbAcsfAQHLP8mAQvsAAfwUXwQyNAH6QNIAAQHRlcghzxbJkW3iyIAQAcsFUATPFnD6AnABy2qCENFzVAAByx9QBAHLPyP6RDDAAI41+ChEBHBUYAQTFQPIywNY+gIBzxYBzxbJIcjLARP0ABL0AMsAyfkAcHTIywLKB8v/ydASzxaXMWwScAHLAeL0AMkNBPiCEGUB81S6jiIxNDZRRccF8uBJAvpA0RA0AshQBfoCUAPPFgHPFszMye1U4CWCEPuI4Rm6jiEyNDYD0VExxwXy4EmLAlUSyFAF+gJQA88WAc8WzMzJ7VTgNCSCECNcr1K64wI3I4IQy4YpArrjAjZbIIIQJQjWarrjAmwxDg8QEQAIgFD7AALsMDEyUDPHBfLgSfpA+gDU0SDQ0x8BAYBA1yEhghAPin6luo5NNiCCEFlfB7y6jiwwBPoAMfpAMfQB0SD4OSBulDCBFp/ecYEC8nD4OAFw+DaggRp3cPg2oLzysI4TghDu0jbTupUE0wMx0ZQ08sBI4uLjDVADcBITAEQzUULHBfLgSchQA88WyRNEQMhQBfoCUAPPFgHPFszMye1UAB4wAscF8uBJ1NTRAe1U+wQAGIIQ03IVjLrchA/y8ADOMfoAMfpAMfpAMfQB+gAg1wsAmtdLwAEBwAGw8rGRMOJUQhYhkXKRceL4OSBuk4EkJ5Eg4iFulDGBKHORAeJQI6gToHOBA6Nw+DygAnD4NhKgAXD4NqBzgQQJghAJZgGAcPg3oLzysADAghA7msoAcPsC+ChFBHBUYAQTFQPIywNY+gIBzxYBzxbJIcjLARP0ABL0AMsAySD5AHB0yMsCygfL/8nQyIAYAcsFAc8WWPoCAphYd1ADy2vMzJcwAXFYy2rM4smAEfsAACW9mt9qJofQB9IH0gampoiBIvgkAgJxFhcAha289qJofQB9IH0gampoii+CfBQAuCowAgmKgeRlgax9AQDniwDni2SQ5GWAifoACXoAZYBk/IA4OmRlgWUD5f/k6EAAz68W9qJofQB9IH0gampov5noNsF4OHLr21FNnJfCg7fwrlF5Ap4rYRnDlGJxnk9G7Y90E+YseApBeHdAfpePAaQHEUEbGst3Opa92T+oO7XKhDUBPIxLOskfRYm0eAo4ZGWD+gBkoYBA",
//                "te6ccgEBBAEAdQACU3BF6oGrY8sIAMiB/HjSggcHLHKKLniWIo834XNprhIcsO73tLA4XzMwQAECCEICj0Utek39dAZraCNlF3JZ7QVzRDW+drX9S9XYryt8PWgBAAMAPmh0dHBzOi8vdGV0aGVyLnRvL3VzZHQtdG9uLmpzb24=",
//                block::StdAddress::parse("EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs").move_as_ok()
//        },
//        Tokens{
//                "tsTON",
//                "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",
//                "te6ccgECKAEABYoAAmF53mxKlxv6SAFIti/lCBJFNvBsHGUghB4nyfxB8hr8V/GJWM4GSzxF9AAAAAAAAAABAQIBAwDAAwEU/wD0pBP0vPLICw4CASAEBQFDv/hy69tRTZyXwoO38K5ReQKeK2EZw5RicZ5PRu2PdBPmQAYCASAHCABQAGh0dHBzOi8vdG9uc3Rha2Vycy5jb20vamV0dG9uL21ldGEuanNvbgIBIAkKAUK/roD9Lx4DSA4igjY1lu51LXuyf1B3a5UIagJ5GJZ1kj4NAUG/RUam/+G3nP3Ya609uHQxPc3i+wXmp0qn81UtlhfHnRMLAUG/btT5QqeEjOLLBmt3oRKMah/4xD9Dii3OJGErqf+riwMMAB4AVG9uc3Rha2VycyBUT04ADAB0c1RPTgAEADkCAWIPEAICyhESAgEgJSYCl9QHQ0wMBcbCSXwTg+kAwAdMfAQHTPwEighAYLY3dup4yMzPwI/hCxwXy4PTwJuDwJCKCEG7bGIm64wIxNDGCEGYXOkW64wJbhA/y8ITFAIBYh8gBHQy+kD4QogjWXBUcAAkEDUQRxA2WchQBvoCUATPFljPFswB+gIBAcsvySLIywES9AD0AMsAyds8+CiIJxwVFgDw+CP4RLzy4Pb4SfLQ+G34RcAAnzD4RtD6APQEMFIivvLg9974RcABnDCCCX14QL7y4Pf4RpEx4siAEAHLBfhCzxZw+gJwActqghBPD3UQAcsfWAHLP/hDAcs/+EQByy/4RQHLP/hH+gL4SPoC9AB/+GnwJcmAQPsAART/APSkE/S88sgLFwPeAnACyFjPFgHPFnD6AnD6AskhyMsBE/QAEvQAywDJ2zwUxwXy4PX4I/hEufLg+QLTLwGBDzL4RBK+8vT6ANIAAQHSAAEB0QGW+EdYoPhnlvhIWKD4aOIEggnJw4AEoRO2CXD7AgLjD/AlyYEAgvsAHB0eAgFiGBkBQtAzMdDTAwFxsJFb4PpAMAHTHwGCECvWNwS64wJbhA/y8BoAHaFxl9qJofSB9IH0AfQAYQH+7UTQ+kD6QPoA+gAwUjbHBfLh9APTPwEB+kDTLwEB+gDSAAEB0gABMVEooSmhIZNRiKCVUZmgCQjiKML/8uH1ggiYloBw+wImEDhAGshQBM8WWM8WAfoCAfoCye1UyIAQAcsFUATPFnD6AnABy2qCEG7bGIkByx9YAcs/Ac8WARsAJgHLL1j6AlgBygABAcoAyYMG+wAAHPkAdMjLAnABygfL/8nQAErIgBgBywX4Qs8WcPoCcAHLaoIKIv3LAcsfAQHLP/hDAcs/Ac8WADrIgBABywVYzxZw+gJwActqghDVMnbbAcsfAQHLPwAx92omhpAAD8MPwg+Wh5/SAA/DFpn4CY/DHAIBICEiAgEgIyQAt00y8B+GTTPwH4ZdQB+Gb6QDD4an/4YXD4Z3D4aHD4afAlggnJw4Bw+wLIgBgBywX4Qs8WcPoCcAHLaoIQjiq7IwHLHwEByz/4QwHLP/hEAcsv+ErPFsmBAIL7AIAHc7UTQ0gAB+GGBDzH4QfL0+kAB+GLTPwH4Y9MvAfhk0z8B+GXUAfhm+gAB+Gf6AAH4aNIAAfhp+kAw+GqAAYT4Rsj4QQHKAPhCzxb4QwHLP/hEAcsv+EUByz/M+Ef6AvhI+gL4SQHKAPhKzxbJ7VSAAj726X2onai9qPJrfgR9rP2svaySXgSdqD2+ID5f/wjaH0AegJ8IPwk/CF8JXwh/CJ8Ivwj/CQIRYg9CDSILAgjiBsIEogSCBHAFjv8YnaidqL2o8mt+BH2s/ay9rJJeBJ2oPb4gPl//CD8IXwh/CJ8IvwjRHwj/CR8JPwlQnCEICEr67DcjiArfib3IeJUfha7nrrsk09lfRnyLnbWK+yHg=",
//                block::StdAddress::parse("EQC98_qAmNEptUtPc7W6xdHh_ZHrBUFpw5Ft_IzNU20QAJav").move_as_ok()
//        },
//        Tokens{
//                "jUSDT",
//                "te6ccgECFgEABIwAART/APSkE/S88sgLAQIBYgIDAgLLBAUCAWoQEQS30IMcAkl8D4AHQ0wMBcbCSXwPg+kD6QDH6ADFx1yH6ADH6ADBzqbQAItMf0z9Z7UTQ+gDU1NHbPFB4Xwcq+kQpwBXjAlsnghB73ZfeuuMCMDFsIjcCghAsdrlzuoGBwgJAgEgDg8AXIBP+DMgbpUwgLH4M94gbvLSmtDTBzHT/9P/9ATTB9Qw0PoA+gD6APoA+gD6ADAB5jM4OTk5OQPA/1Fnuhaw8uGTAvpA+CdvEFAFoQT6APoA0XDIghAXjUUZWAcCyx/LPyL6AnDIywHJ0M8Wf1AJdMjLAhLKB8v/ydAYzxZQB/oCE8sAyVRGRPAcAqBmA8hQA/oCzMzJ7VSCEMBmDM/IWPoCyXAKAf4xNTU2OAP6APpAMPgoJnBUIBNUFAPIUAT6AljPFgHPFszJIsjLARL0APQAywDJcAH5AHTIywISygfL/8nQUAbHBfLhlFAkoVRgZMhQA/oCzMzJ7VTIUATPFhTMyfgnbxBYociAEAHLBVADf3RQA8sCEsoHy/9Y+gJxActqzMlwCwEQ4wJfB4QP8vAMAEiDB3GADMjLA8sBywgTy/8ClXFYy2HMmHBYy2EB0M8W4slw+wAAZPsAghDAdwzPyFj6AslwgwdxgAzIywPLAcsIE8v/ApVxWMthzJhwWMthAdDPFuLJcPsAAfxQNaAVvPLgSwP6QNMA0ZXIIc8WyZFt4siAGAHLBVADzxZw+gJwActqghDRc1QAWAQCyx/LPyL6RDDAAI42+ChDBHBUIBNUFAPIUAT6AljPFgHPFszJIsjLARL0APQAywDJcAH5AHTIywISygfL/8nQEs8WlmwicAHLAeL0AMkNAAiAQPsAAGe7kODeARwuoodSGEGEEyVMryVMYcQq3xgDSEmAACXMZmZFOEVKpEDfAgOWDgVKBcjYQ5OhAJm58FCIBuCoQCaoKAeQoAn0BLGeLAOeLZmSRZGWAiXoAegBlgGSQOAD8gDpkZYEJZQPl/+ToZEAMAOWCgOeLKAH9ATuA5bWJZmZkuH2AQIBWBITACm0aP2omh9AGpqaJgY6GmP6c/pg+jAAfa289qJofQBqami2EPwUALgqEAmqCgHkKAJ9ASxniwDni2ZkkWRlgIl6AHoAZYBkuAD8gDpkZYEJZQPl/+ToQAHhrxb2omh9AGpqaLaBaGmP6c/pg+i9eAqBPXgKgMAIeArkRoOtDo6ODmdF5exOTSyM7KXOje3Fze5M5e6N7WytxfBniyxni0WZeYPEZ4sA54sQRalzU5t7dGeLZOgAxaFzg3M8Z4tk6AC4ZGWDgOeLZMAUAcSC8HDl17aimzkvhQdv4Vyi8gU8VsIzhyjE4zyejdse6CfMWAWDB/QXA3DIywcBzxbJgvBhBdbMdq9AAyXpTViM5RG+W/27c7Q33FHspDkX16Q+PVgEgwf0FwJwyMsHAc8WyRUAcILw7oD9Lx4DSA4igjY1lu51LXuyf1B3a5UIagJ5GJZ1kj5YA4MH9BdwyMsH9ADJf3DIywHJ0EAD",
//                "te6ccgECFQEAA6sAAg1gE6h/mJKoAQIAMgAAAAHawX+VjS7lI6IgYgaZRZfBPYMexwYBFP8A9KQT9LzyyAsDAgFiBAUCAssGBwAboPYF2omh9AH0gfSBqaMCAc4ICQIBWAwNAvcIMcAkl8E4AHQ0wMBcbCVE18D8B3g+kD6QDH6ADFx1yH6ADH6ADBzqbQAAtMfAds8WzI0NDQkghAPin6lupowbCI2XjEQI/Aa4CSCEBeNRRm6mzBsIl4yECRDAPAb4DdbNoIQWV8HvLqfAnGw8tLAUCO68uLGAfAc4F8FgCgsAET6RDDAAPLhTYABcgE/4MyBulTCAsfgz3iBu8tKa0NMHMdP/0//0BNMH1DDQ+gD6APoA+gD6APoAMAAIhA/y8AIBWA4PAgFIExQB9wF0z8BAfoA+kAh8AHtRND6APpA+kDU0VE2oVIsxwXy4sEqwv/y4sJUNEJwVCATVBQDyFAE+gJYzxYBzxbMySLIywES9AD0AMsAySBwAfkAdMjLAhLKB8v/ydAE+kD0BDH6ACDXScIA8uLEyIAYAcsFUAfPFnD6AncBy2uAQAvM7UTQ+gD6QPpA1NEK0z8BAfoAUVGgBfpA+kBTXccFVHNvcFQgE1QUA8hQBPoCWM8WAc8WzMkiyMsBEvQA9ADLAMlwAfkAdMjLAhLKB8v/ydBQD8cFHrHy4sMM+gBRyqEptggZoVAHoBihJpJsVeMNJdcLAcMAIcIAsIBESAKoTzMiCEBeNRRlYCgLLH8s/UAf6AiLPFlAGzxYl+gJQA88WyVAFzCORcpFx4lAHqBOgCKoAUASgF6AUvPLixQHJgED7AEMAyFAE+gJYzxYBzxbMye1UAHJSaaAYociCEHNi0JwpAssfyz9QB/oCUATPFlAHzxbJyIAQAcsFJ88WUAT6AnEBy2oTzMlx+wBQQhMAdI4jyIAQAcsFUAbPFlAF+gJwActqghDVMnbbWAUCyx/LP8ly+wCSWzPiQAPIUAT6AljPFgHPFszJ7VQA6ztRND6APpA+kDU0QXTPwEB+gAhwgDy4sL6QPQEAdDTn9EB0VFioVJYxwXy4sEmwv/y4sLIghB73ZfeWAQCyx/LPwH6AiPPFgHPFhPLn8nIgBgBywUjzxZw+gJxActqzMmAQPsAQBPIUAT6AljPFgHPFszJ7VSAAhyAINch7UTQ+gD6QPpA1NEE0x8BhA8hghAXjUUZugKCEHvdl966ErHy9NM/ATD6ADAToFAjyFAE+gJYzxYBzxbMye1Ug",
//                block::StdAddress::parse("EQBynBO23ywHy_CgarY9NK9FTz0yDsG82PtcbSTQgGoXwiuA").move_as_ok()
//        },
//        Tokens{
//                "jUSDC",
//                "te6ccgECFgEABIwAART/APSkE/S88sgLAQIBYgIDAgLLBAUCAWoQEQS30IMcAkl8D4AHQ0wMBcbCSXwPg+kD6QDH6ADFx1yH6ADH6ADBzqbQAItMf0z9Z7UTQ+gDU1NHbPFB4Xwcq+kQpwBXjAlsnghB73ZfeuuMCMDFsIjcCghAsdrlzuoGBwgJAgEgDg8AXIBP+DMgbpUwgLH4M94gbvLSmtDTBzHT/9P/9ATTB9Qw0PoA+gD6APoA+gD6ADAB5jM4OTk5OQPA/1Fnuhaw8uGTAvpA+CdvEFAFoQT6APoA0XDIghAXjUUZWAcCyx/LPyL6AnDIywHJ0M8Wf1AJdMjLAhLKB8v/ydAYzxZQB/oCE8sAyVRGRPAcAqBmA8hQA/oCzMzJ7VSCEMBmDM/IWPoCyXAKAf4xNTU2OAP6APpAMPgoJnBUIBNUFAPIUAT6AljPFgHPFszJIsjLARL0APQAywDJcAH5AHTIywISygfL/8nQUAbHBfLhlFAkoVRgZMhQA/oCzMzJ7VTIUATPFhTMyfgnbxBYociAEAHLBVADf3RQA8sCEsoHy/9Y+gJxActqzMlwCwEQ4wJfB4QP8vAMAEiDB3GADMjLA8sBywgTy/8ClXFYy2HMmHBYy2EB0M8W4slw+wAAZPsAghDAdwzPyFj6AslwgwdxgAzIywPLAcsIE8v/ApVxWMthzJhwWMthAdDPFuLJcPsAAfxQNaAVvPLgSwP6QNMA0ZXIIc8WyZFt4siAGAHLBVADzxZw+gJwActqghDRc1QAWAQCyx/LPyL6RDDAAI42+ChDBHBUIBNUFAPIUAT6AljPFgHPFszJIsjLARL0APQAywDJcAH5AHTIywISygfL/8nQEs8WlmwicAHLAeL0AMkNAAiAQPsAAGe7kODeARwuoodSGEGEEyVMryVMYcQq3xgDSEmAACXMZmZFOEVKpEDfAgOWDgVKBcjYQ5OhAJm58FCIBuCoQCaoKAeQoAn0BLGeLAOeLZmSRZGWAiXoAegBlgGSQOAD8gDpkZYEJZQPl/+ToZEAMAOWCgOeLKAH9ATuA5bWJZmZkuH2AQIBWBITACm0aP2omh9AGpqaJgY6GmP6c/pg+jAAfa289qJofQBqami2EPwUALgqEAmqCgHkKAJ9ASxniwDni2ZkkWRlgIl6AHoAZYBkuAD8gDpkZYEJZQPl/+ToQAHhrxb2omh9AGpqaLaBaGmP6c/pg+i9eAqBPXgKgMAIeArkRoOtDo6ODmdF5exOTSyM7KXOje3Fze5M5e6N7WytxfBniyxni0WZeYPEZ4sA54sQRalzU5t7dGeLZOgAxaFzg3M8Z4tk6AC4ZGWDgOeLZMAUAcSC8HDl17aimzkvhQdv4Vyi8gU8VsIzhyjE4zyejdse6CfMWAWDB/QXA3DIywcBzxbJgvBhBdbMdq9AAyXpTViM5RG+W/27c7Q33FHspDkX16Q+PVgEgwf0FwJwyMsHAc8WyRUAcILw7oD9Lx4DSA4igjY1lu51LXuyf1B3a5UIagJ5GJZ1kj5YA4MH9BdwyMsH9ADJf3DIywHJ0EAD",
//                "te6ccgECFQEAA6oAAgtRizUOC7gBAgAyAAAAAaC4aZHGIYs2wdGdSi6esM42ButIBgEU/wD0pBP0vPLICwMCAWIEBQICywYHABug9gXaiaH0AfSB9IGpowIBzggJAgFYDA0C9wgxwCSXwTgAdDTAwFxsJUTXwPwHeD6QPpAMfoAMXHXIfoAMfoAMHOptAAC0x8B2zxbMjQ0NCSCEA+KfqW6mjBsIjZeMRAj8BrgJIIQF41FGbqbMGwiXjIQJEMA8BvgN1s2ghBZXwe8up8CcbDy0sBQI7ry4sYB8BzgXwWAKCwARPpEMMAA8uFNgAFyAT/gzIG6VMICx+DPeIG7y0prQ0wcx0//T//QE0wfUMND6APoA+gD6APoA+gAwAAiED/LwAgFYDg8CAUgTFAH3AXTPwEB+gD6QCHwAe1E0PoA+kD6QNTRUTahUizHBfLiwSrC//LiwlQ0QnBUIBNUFAPIUAT6AljPFgHPFszJIsjLARL0APQAywDJIHAB+QB0yMsCEsoHy//J0AT6QPQEMfoAINdJwgDy4sTIgBgBywVQB88WcPoCdwHLa4BAC8ztRND6APpA+kDU0QrTPwEB+gBRUaAF+kD6QFNdxwVUc29wVCATVBQDyFAE+gJYzxYBzxbMySLIywES9AD0AMsAyXAB+QB0yMsCEsoHy//J0FAPxwUesfLiwwz6AFHKoSm2CBmhUAegGKEmkmxV4w0l1wsBwwAhwgCwgERIAqhPMyIIQF41FGVgKAssfyz9QB/oCIs8WUAbPFiX6AlADzxbJUAXMI5FykXHiUAeoE6AIqgBQBKAXoBS88uLFAcmAQPsAQwDIUAT6AljPFgHPFszJ7VQAclJpoBihyIIQc2LQnCkCyx/LP1AH+gJQBM8WUAfPFsnIgBABywUnzxZQBPoCcQHLahPMyXH7AFBCEwB0jiPIgBABywVQBs8WUAX6AnABy2qCENUydttYBQLLH8s/yXL7AJJbM+JAA8hQBPoCWM8WAc8WzMntVADrO1E0PoA+kD6QNTRBdM/AQH6ACHCAPLiwvpA9AQB0NOf0QHRUWKhUljHBfLiwSbC//LiwsiCEHvdl95YBALLH8s/AfoCI88WAc8WE8ufyciAGAHLBSPPFnD6AnEBy2rMyYBA+wBAE8hQBPoCWM8WAc8WzMntVIACHIAg1yHtRND6APpA+kDU0QTTHwGEDyGCEBeNRRm6AoIQe92X3roSsfL00z8BMPoAMBOgUCPIUAT6AljPFgHPFszJ7VSA=",
//                block::StdAddress::parse("EQB-MPwrd1G6WKNkLz_VnV6WqBDd142KMQv-g1O-8QUA3728").move_as_ok()
//        }
//};
//
//auto highload_code_cell = vm::std_boc_deserialize(
//        td::base64_decode(
//                td::Slice(
//                        "te6ccgECDgEAAeMAART/APSkE/S88sgLAQIBIAIDAgFIBAUB6vKDCNcYINMf0z/4I6ofUyC58mPtRNDTH9M/0//0BNFTYIBA9A5voTHyYFFzuvKiB/kBVBCH+RDyowL0BNH4AH+OFiGAEPR4b6UgmALTB9QwAfsAkTLiAbPmW4MlochANIBA9EOK5jEByMsfE8s/y//0AMntVA0CAs4GBwIBIAsMAANDCAIBIAgJAfcINdKIP4gMCDAAJEw4MiLpDZWxsW2RhdGE9jPFiLPFotyxyZWZzPVuM8WcZNTAruOQAPUAdDwAiTDAZcCixLIzxYC3iTIbZ0CeqkMpjBQA28CIsAA5jKYAW8iAssHIW7mMcnQE88WixPYzxZYzxYDpBPoE18DiyXV2M8WgCgDdCBulzCLRudWxsjgIJqUcKAwf5JbcPL/2hGOH8htnQJ6qQymMFADbwIiwADmMpgBbyICywchbuYxydDgIJqU10kwf5JbcPL/2hGS8ALgIJmT0DB/kltw8v/aEZPQ8ALgMIvHVua25vd24gdHlwZYgAATJ0AAXvZznaiaGmvmOuF/8AEG+X5dqJoaY+Y6Z/p/5j6AmipEEAgegc30JjJLb/JXdHxQANCCAQPSWb6VsEiCUMFMDud4gkzM2AZJsIeKz"
//                )
//        )
//                .move_as_ok()
//)
//        .move_as_ok();
//
//block::StdAddress build_highload_address(td::Ed25519::PrivateKey *pk, int walletNumber) {
//    auto data_cell = vm::CellBuilder()
//            .store_long(walletNumber, 32)
//            .store_long(0, 64)
//            .store_bytes(pk->get_public_key().move_as_ok().as_octet_string().as_mutable_slice())
//            .store_long(0, 1)
//            .finalize();
//    ton::SmartContract smc({highload_code_cell, data_cell});
//    return smc.get_address(0);
//}
//
//block::StdAddress calculate_wallet(const block::StdAddress *address, Tokens *t) {
//    vm::CellBuilder anycast_cb;
//    anycast_cb.store_bool_bool(false);
//    auto anycast_cell = anycast_cb.finalize();
//    td::Ref<vm::CellSlice> anycast_cs = vm::load_cell_slice_ref(anycast_cell);
//    vm::CellBuilder cb;
//    block::gen::t_MsgAddressInt.pack_addr_std(cb, anycast_cs, 0, address->addr);
//
//    auto cc = vm::std_boc_deserialize(td::base64_decode(td::Slice(t->code)).move_as_ok()).move_as_ok();
//    auto dc = vm::std_boc_deserialize(td::base64_decode(td::Slice(t->data)).move_as_ok()).move_as_ok();
//
//    ton::SmartContract smc1({cc, dc});
//    ton::SmartContract::Args args;
//    args.set_vm_verbosity_level(-1);
//    args.set_address(t->address);
//    args.set_stack({vm::StackEntry(vm::load_cell_slice_ref(cb.finalize()))});
//    args.set_method_id("get_wallet_address");
//
//    auto res = smc1.run_get_method(args);
//    auto stack = res.stack->as_span();
//    auto ref = stack[0].as_slice();
//    auto sl = *(ref.get());
//    block::gen::MsgAddressInt::Record_addr_std dest;
//    assert(block::gen::t_MsgAddressInt.unpack(sl, dest));
//    return block::StdAddress(dest.workchain_id, dest.address);
//}
//
//int max_same_prefix(const block::StdAddress *liq, const block::StdAddress *liq_w, const block::StdAddress *evaa_w) {
//    unsigned long long l = liq->addr.bits().get_uint(32);
//    unsigned long long lw = liq_w->addr.bits().get_uint(32);
//    unsigned long long ew = evaa_w->addr.bits().get_uint(32);
//    unsigned long long i = ((unsigned long long) 1) << ((unsigned long long) 31);
//    int prefix = 0;
//    for (int cnt = 0; cnt < 32; cnt++) {
//        if ((l & i) == (lw & i) && (lw & i) == (ew & i)) {
//            prefix++;
//            i = i >> 1;
//        } else {
//            break;
//        }
//    }
//    return prefix;
//}
//
//std::string to_bin_pad_32(unsigned long long x) {
//    char array[32];
//    for (char &i: array) {
//        i = '0';
//    }
//    int idx = 31;
//    while (x > 0) {
//        if (x % 2) {
//            array[idx] = '1';
//        } else {
//            array[idx] = '0';
//        }
//        x = x / 2;
//        idx--;
//    }
//    return {array, 32};
//}
//
//void do_report(std::map<std::string, std::tuple<td::Ed25519::PrivateKey *, int, int>> &map) {
//    std::cout << "Report:" << std::endl;
//    for (auto k: map) {
//        auto address = build_highload_address(std::get<0>(k.second), std::get<1>(k.second));
//
//        std::cout << k.first
//                  << "-> address: " << address.rserialize(true)
//                  << " private key: " << td::hex_encode(std::get<0>(k.second)->as_octet_string().as_slice())
//                  << " public key: "
//                  << td::hex_encode(std::get<0>(k.second)->get_public_key().move_as_ok().as_octet_string().as_slice())
//                  << " number: " << std::get<1>(k.second)
//                  << " longest prefix: " << std::get<2>(k.second)
//                  << std::endl;
//    }
//    std::cout << "End Report" << std::endl;
//}
//
//int main(int argc, char *argv[]) {
//    td::set_verbosity_level(0);
//
//    block::StdAddress items[3] = {
//            block::StdAddress::parse("UQBOG8933MiA1-DqOhBr5NM1qmyPHE4BJrgfQ3tmeZGkU4zZ").move_as_ok(),
//            block::StdAddress::parse("UQD_-sDcQmn1aYc59o0sRBSWxkCaV1wrsw-VYBk9Vi2T0VLv").move_as_ok(),
//            block::StdAddress::parse("UQCRiQJzcSACYAkIzFNpY1gqN-omHEzKnFF0sPCoq3hpQ946").move_as_ok()
//    };
//
//    //auto ton_address = block::StdAddress::parse("EQC8rUA9u5GDd9TvXdsTL26BBA3BKwpZMwyjQvljPXJRNyzF").move_as_ok();
//
//    for (int i = 0; i < 3; i++) {
//        auto addr = items[i];
//        auto t = master_contract_tokens[i];
//        auto wallet_liq = calculate_wallet(&addr, &t);
//        auto wallet_evaa = calculate_wallet(&evaa_contract, &t);
//        std::cout << addr.rserialize(true) << " " << wallet_liq.rserialize(true) << " " << t.address.rserialize(true) << "\n";
//
////        auto a = addr.addr.bits().get_uint(32);
////        auto b = wallet_liq.addr.bits().get_uint(32);
////        auto c = wallet_evaa.addr.bits().get_uint(32);
////        std::cout << t.name << " " << to_bin_pad_32(a) << " " << to_bin_pad_32(b) << " " << to_bin_pad_32(c) << std::endl;
//    }
//
//    /*
//     token   EQDNhy-nxYFgUqzfUzImBEP67JqsyMIcyk2S5_RwNNEYku0k
//    wallet  UQBOG8933MiA1-DqOhBr5NM1qmyPHE4BJrgfQ3tmeZGkU4zZ
//    jwallet UQBOEkPeRALTNm8ZxZ6jXHLjP-RHfmN15ZCKPqRaZwVwcCre
//
//    token   EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs
//    wallet  UQD_-sDcQmn1aYc59o0sRBSWxkCaV1wrsw-VYBk9Vi2T0VLv
//    jwallet UQD_brJ6W5MAoeq3VPmpehv1bbPkDdsdKmhC4OjtqEmzrWEp
//
//    token   EQC98_qAmNEptUtPc7W6xdHh_ZHrBUFpw5Ft_IzNU20QAJav
//    wallet  UQCRiQJzcSACYAkIzFNpY1gqN-omHEzKnFF0sPCoq3hpQ946
//    jwallet UQCRF6FveGpCGxJzx83J769iBT1_W5B14h0rb_2jEA0x04-N
//     */
//
//
////
////
////    block::StdAddress items[5] = {
////            block::StdAddress::parse("EQBOyiE315r1b3ttjT_k_LIOTpL24kLqKFl2KnJdkBYBI1cS").move_as_ok(),
////            block::StdAddress::parse("EQD_c6vsSW-CX0_MVWb7XiZRXvHvJbRjbKgzelmi9Py1b7Td").move_as_ok(),
////            block::StdAddress::parse("EQCRdILrOKT6P41cmvs5P_ZJOdDqqarcY5UAd-FPXzgmXwr4").move_as_ok(),
////            block::StdAddress::parse("EQBwTmOBxn5XaHMMb5IGrJq_m9xpVqU_FU2rabNuNyiePnd6").move_as_ok(),
////            block::StdAddress::parse("EQCEEK-Q9roTKhQTYic9VRzJaZzen603MLuFvCXbfUh6vUWw").move_as_ok()
////    };
////
////    auto ton_address = block::StdAddress::parse("EQC8rUA9u5GDd9TvXdsTL26BBA3BKwpZMwyjQvljPXJRNyzF").move_as_ok();
////
////    for (int i = 0; i < 5; i++) {
////        auto addr = items[i];
////        auto t = master_contract_tokens[i];
////        auto wallet_liq = calculate_wallet(&addr, &t);
////        auto wallet_evaa = calculate_wallet(&evaa_contract, &t);
////
////        auto a = addr.addr.bits().get_uint(32);
////        auto b = wallet_liq.addr.bits().get_uint(32);
////        auto c = wallet_evaa.addr.bits().get_uint(32);
////        std::cout << t.name << " " << to_bin_pad_32(a) << " " << to_bin_pad_32(b) << " " << to_bin_pad_32(c) << std::endl;
////    }
////    auto a = evaa_contract.addr.bits().get_uint(32);
////    auto b = ton_address.addr.bits().get_uint(32);
////    std::cout << to_bin_pad_32(a) << " " << to_bin_pad_32(b) << std::endl;
////
////    auto key = td::Ed25519::generate_private_key().move_as_ok();
////
////    // number, best_prefix
////    std::map<std::string, std::tuple<td::Ed25519::PrivateKey *, int, int>> best_report;
////
////    auto ton = "TON";
////    best_report.insert(std::pair(ton, std::make_tuple(&key, 0, 0)));
////    for (const auto &t: master_contract_tokens) {
////        best_report.insert(std::pair(t.name, std::make_tuple(&key, 0, 0)));
////    }
////
////    int max_number = 1 << 30;
////    for (int i = 0; i < max_number; i++) {
////        auto address = build_highload_address(&key, i);
////        auto value = best_report.at(ton);
////        auto curr_prefix = max_same_prefix(&address, &address, &evaa_contract);
////        auto best_prefix = std::get<2>(value);
////        if (best_prefix < curr_prefix) {
////            best_report[ton] = std::make_tuple(&key, i, curr_prefix);
////        }
////
////        for (auto t: master_contract_tokens) {
////            auto wallet_liq = calculate_wallet(&address, &t);
////            auto wallet_evaa = calculate_wallet(&evaa_contract, &t);
////            value = best_report.at(t.name);
////            curr_prefix = max_same_prefix(&address, &wallet_liq, &wallet_evaa);
////            best_prefix = std::get<2>(value);
////            if (best_prefix < curr_prefix) {
////                best_report[t.name] = std::make_tuple(&key, i, curr_prefix);
////            }
////        }
////
////        if (i % 10000 == 0) {
////            do_report(best_report);
////        }
////    }
////    do_report(best_report);
//    return 0;
//}
//
//
////std::string to_bin_pad_32(unsigned long long x) {
////    char array[32];
////    for (char &i: array) {
////        i = '0';
////    }
////    int idx = 31;
////    while (x > 0) {
////        if (x % 2) {
////            array[idx] = '1';
////        } else {
////            array[idx] = '0';
////        }
////        x = x / 2;
////        idx--;
////    }
////    return {array};
////}
//
////char *HexToBytes(const std::string &hex) {
////    char *cc = new char[hex.length() / 2];
////    for (unsigned int i = 0; i < hex.length(); i += 2) {
////        std::string byteString = hex.substr(i, 2);
////        char byte = (char) strtol(byteString.c_str(), NULL, 16);
////        cc[i / 2] = byte;
////    }
////    return cc;
////}
//
////    char *v = HexToBytes(
////            "da762cbfd96dd8795c084ba999d98ed22c43f23defdb3717cec03513f1ccd02c523b42413de1e46d8cd90d9c18ea04fbe0d219e59c8b36eb09e98ae63f5a0b79");
////    auto slice = td::Slice(v, 32);
////    auto kk = td::SecureString(td::UniqueSlice(std::move(slice)));
////    auto key = td::Ed25519::PrivateKey(std::move(kk));
//
//
////#include "td/utils/port/signals.h"
////#include "td/utils/OptionParser.h"
////#include "td/utils/format.h"
////#include "td/utils/logging.h"
////#include "td/utils/check.h"
////
////#include "crypto/vm/cp0.h"
////
////#include "InsertManagerPostgres.h"
////#include "DataParser.h"
////#include "DbScanner.h"
////#include "EventProcessor.h"
////#include "IndexScheduler.h"
////#include "json/json.h"
////#include <json/reader.h>
////#include <json/writer.h>
////#include <json/value.h>
////#include <string>
////#include <fstream>
////
////std::vector<Json::Value> parse_items(std::string p) {
////  std::ifstream file(p);
////  std::string line;
////  std::vector<Json::Value> values;
////  JSONCPP_STRING err;
////  if (file.is_open()) {
////    int cnt = 0;
////    while (std::getline(file, line)) {
////      const auto rawJsonLength = static_cast<int>(line.length());
////      Json::Value root;
////      Json::CharReaderBuilder builder;
////      const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
////      if (!reader->parse(line.c_str(), line.c_str() + rawJsonLength, &root,
////                         &err)) {
////        std::cout << "error: " << err << std::endl;
////        std::_Exit(EXIT_FAILURE);
////      }
////      cnt += 1;
////      values.push_back(root);
////    }
////    file.close();
////  } else {
////    std::cerr << "Unable to open file" << std::endl;
////    std::_Exit(1);
////  }
////  return values;
////}
////
////int main(int argc, char *argv[]) {
////  auto lizardMsg =
////      "te6cckEBBAEAowABmR6Nyrs35sD5QkvOpB1frSm9GCVoTHuOMPZ0FSuUjcWAhc04zhKNLe9rT8cuOn+cQZGBzKftQT87Iy3GeSo+8gcAAAAAZxQZ0wAAAADAAQEHoAAEBwIBaEIARqZ2hsOjPaBuSK2oJaz8mnj+zoB2HEWdePth3Ryh8HAgF9eEAAAAAAAAAAAAAAAAAAEDACYAAAAAaGVsbG8gcGFyZW50ICMy/I6S4Q==";
////  auto lizardMsgCell = vm::std_boc_deserialize(td::base64_decode(td::Slice(lizardMsg)).move_as_ok()).move_as_ok();
////
////  auto code =
////      "te6ccgECDgEAAeMAART/APSkE/S88sgLAQIBIAIDAgFIBAUB6vKDCNcYINMf0z/4I6ofUyC58mPtRNDTH9M/0//0BNFTYIBA9A5voTHyYFFzuvKiB/kBVBCH+RDyowL0BNH4AH+OFiGAEPR4b6UgmALTB9QwAfsAkTLiAbPmW4MlochANIBA9EOK5jEByMsfE8s/y//0AMntVA0CAs4GBwIBIAsMAANDCAIBIAgJAfcINdKIP4gMCDAAJEw4MiLpDZWxsW2RhdGE9jPFiLPFotyxyZWZzPVuM8WcZNTAruOQAPUAdDwAiTDAZcCixLIzxYC3iTIbZ0CeqkMpjBQA28CIsAA5jKYAW8iAssHIW7mMcnQE88WixPYzxZYzxYDpBPoE18DiyXV2M8WgCgDdCBulzCLRudWxsjgIJqUcKAwf5JbcPL/2hGOH8htnQJ6qQymMFADbwIiwADmMpgBbyICywchbuYxydDgIJqU10kwf5JbcPL/2hGS8ALgIJmT0DB/kltw8v/aEZPQ8ALgMIvHVua25vd24gdHlwZYgAATJ0AAXvZznaiaGmvmOuF/8AEG+X5dqJoaY+Y6Z/p/5j6AmipEEAgegc30JjJLb/JXdHxQANCCAQPSWb6VsEiCUMFMDud4gkzM2AZJsIeKz";
////  auto data =
////      "te6ccgEBAgEAPAABWQAAAABnAYMxAAAAALQDyN7m3oODrjcBZORPBptRX6YG/uYGO3tbfUGE3MIqwAEAE6AzgMN/gAAAAEA=";
////
////  auto lizardAddr = block::StdAddress(td::Slice("UQARoubOdzvwu0EGdign_J5ojm1uyJn4SwQ68PgwkYa1Lb5J"));
////  std::cout << lizardAddr.rserialize();
////
////  auto code_cell = vm::std_boc_deserialize(td::base64_decode(td::Slice(code)).move_as_ok()).move_as_ok();
////  auto data_cell = vm::std_boc_deserialize(td::base64_decode(td::Slice(data)).move_as_ok()).move_as_ok();
////  ton::SmartContract smc({code_cell, data_cell});
////  ton::SmartContract::Args args;
////
////  vm::CellBuilder anycast_cb;
////  anycast_cb.store_bool_bool(false);
////  auto anycast_cell = anycast_cb.finalize();
////  td::Ref<vm::CellSlice> anycast_cs = vm::load_cell_slice_ref(anycast_cell);
////
////  vm::CellBuilder cb;
////  block::gen::t_MsgAddressInt.pack_addr_std(cb, anycast_cs, lizardAddr.workchain, lizardAddr.addr);
////  auto owner_address_cell = cb.finalize();
////
////  //args.set_libraries(vm::Dictionary(blocks_ds.config_->get_libraries_root(), 256));
////  //args.set_config(blocks_ds.config_);
////  args.set_now(td::Time::now());
////  args.set_address(
////    block::StdAddress(td::Slice("UQCWlSWQTI1KOl8oVEBZEDX5JFHjlDwVAbQZk9N5UV71EPpy"))
////  );
////  args.set_stack({vm::StackEntry(vm::load_cell_slice_ref(owner_address_cell))});
////
////  //args.set_method_id("get_wallet_address");
////  args.vm_log_verbosity_level = 5;
////  args.limits = vm::GasLimits{1000000, 1000000};
////  // auto res = smc.send_internal_message(lizardMsgCell, args);
////  // std::cout << res.code;
////
////  // 75693 -- get owner address
////  //args.set_method_id(75693); // 74945, 75693
////  auto res = smc.send_external_message(lizardMsgCell, args);
////  std::cout << res.code << std::endl;
////
////  auto stack = res.stack->as_span();
////  auto cnt = res.stack->depth();
////  for (auto i = 0; i < cnt; i++) {
////    std::cout << "!! here!!" << stack[i].type() << std::endl;
////  }
////
////
////  // ----
////
////  // auto a2 = "te6cckECHwEAAtgAAVsPin6lAAAAAAAAAACAHzT08s/AyLLbNObeCN4g8mgY2/7NGAHZDzRsGpmtGd7wAQlGA2rhGNnlLIJfis+p89WmG4DocIJxYVz8bO4ccthkpxChABECAgWBcAIDBChIAQF74DWmVhmM9SnkyaM4ahfHPja5mQMeAXa3H9XtbDVJkgAQAgEgBQYoSAEBXoRdqKQGacICk+FyI0A9f120dnKp77VVVQOQpbSktlgADwIBIAcIKEgBAV822Cc3DYLHgpJ4IiRYwA5laRssGBZinwfUY4ZNUnbkAA4CASAJCihIAQHGCfck4uvjQvvdsfcvbZora+TipEkgOKbsXdFMlQIAJQANAgEgCwwoSAEBRVAnHDn20YgprbXqEh/XEsz3B+i31dJQD/hRuwi5x5gACwIBIA0OAgEgDxAoSAEBpCBe23Ry0PQ8RzAk3DdV4g0hO2vRbFGfo9CMXSMjpyAACgIBIBESKEgBAXmbblGkRE5RjXxJ/pFe442oMlgJQ4puABuZj3/2EDIHAAkoSAEBKlwebKVa6Ol70G0QAAnHVS0r+ChSokBo4XaBbW00UIwACQIBIBMUKEgBAbHJJYYuEs5GcJ5OvGjZm0qaovABuCOP991e2dBouGeUAAYCASAVFgIBIBcYKEgBAV6atPQ5vEcIpf8YkClLAXN0JuGn0ICt0+GmInE3gr70AAUoSAEBxtwNQHBKtEIhgkmaLkculfbZzkxfdU8Po93uuNoGdYMAAwIBIBkaAgJzGxwoSAEBgRPGudhRAN78RA0zlDTyI6p4Qoazgia0Kr3VcVaVcF4AAShIAQHZgwDQMbXLlaH+pz//CrfK/yoXWAzn6zgfICdQXeedOQAAAgFYHR4ARbunln4GRZbZpzbwRvEHk0DG3/ZowA7IeaNg1M1ozvc6iHGoKEgBARpKDUbn5FVILC/CycLqiUpjDUET5Y5yYsqGE/NH+W10AACMG7Ts";
////  // auto a = "te6cckECHwEAAtgAAVsPin6lAAAAAAAAAACAHzT08s/AyLLbNObeCN4g8mgY2/7NGAHZDzRsGpmtGd7wAQlGA2rhGNnlLIJfis+p89WmG4DocIJxYVz8bO4ccthkpxChABECAgWBcAIDBChIAQF74DWmVhmM9SnkyaM4ahfHPja5mQMeAXa3H9XtbDVJkgAQAgEgBQYoSAEBXoRdqKQGacICk+FyI0A9f120dnKp77VVVQOQpbSktlgADwIBIAcIKEgBAV822Cc3DYLHgpJ4IiRYwA5laRssGBZinwfUY4ZNUnbkAA4CASAJCihIAQHGCfck4uvjQvvdsfcvbZora+TipEkgOKbsXdFMlQIAJQANAgEgCwwoSAEBRVAnHDn20YgprbXqEh/XEsz3B+i31dJQD/hRuwi5x5gACwIBIA0eAgEgDh0CASAPEChIAQEqXB5spVro6XvQbRAACcdVLSv4KFKiQGjhdoFtbTRQjAAJAgEgERIoSAEBscklhi4SzkZwnk68aNmbSpqi8AG4I4/33V7Z0Gi4Z5QABgIBIBMcAgEgFBUoSAEBxtwNQHBKtEIhgkmaLkculfbZzkxfdU8Po93uuNoGdYMAAwIBIBYbAgJzFxgoSAEB2YMA0DG1y5Wh/qc//wq3yv8qF1gM5+s4HyAnUF3nnTkAAAIBWBkaAEW7p5Z+BkWW2ac28EbxB5NAxt/2aMAOyHmjYNTNaM73OohxqChIAQEaSg1G5+RVSCwvwsnC6olKYw1BE+WOcmLKhhPzR/ltdAAAKEgBAYETxrnYUQDe/EQNM5Q08iOqeEKGs4ImtCq91XFWlXBeAAEoSAEBXpq09Dm8Rwil/xiQKUsBc3Qm4afQgK3T4aYicTeCvvQABShIAQF5m25RpEROUY18Sf6RXuONqDJYCUOKbgAbmY9/9hAyBwAJKEgBAaQgXtt0ctD0PEcwJNw3VeINITtr0WxRn6PQjF0jI6cgAAr4Extq";
////  // auto trusted = "te6cckECHgEAAqcACUYD0+NKie/NtyBv9cVTxvpNY5eRyWXHrk3RKxHpCgVjz9cAEQEiBYFwAgIDKEgBAc7H9wq9YydfNXTEHVwEcssbYn+Mq3qA74PmynVoXgunABAiASAEBShIAQFehF2opAZpwgKT4XIjQD1/XbR2cqnvtVVVA5CltKS2WAAPIgEgBgcoSAEBXzbYJzcNgseCkngiJFjADmVpGywYFmKfB9Rjhk1SduQADiIBIAgJKEgBAcYJ9yTi6+NC+92x9y9tmitr5OKkSSA4puxd0UyVAgAlAA0iASAKCyhIAQFFUCccOfbRiCmtteoSH9cSzPcH6LfV0lAP+FG7CLnHmAALIgEgDB0iASANHCIBIA4PKEgBASpcHmylWujpe9BtEAAJx1UtK/goUqJAaOF2gW1tNFCMAAkiASAQEShIAQGxySWGLhLORnCeTrxo2ZtKmqLwAbgjj/fdXtnQaLhnlAAGIgEgEhsiASATFChIAQHG3A1AcEq0QiGCSZouRy6V9tnOTF91Tw+j3e642gZ1gwADIgEgFRoiAnMWFyhIAQHZgwDQMbXLlaH+pz//CrfK/yoXWAzn6zgfICdQXeedOQAAIgFYGBkARbunln4GRZbZpzbwRvEHk0DG3/ZowA7IeaNg1M1ozvc6iHGoKEgBARpKDUbn5FVILC/CycLqiUpjDUET5Y5yYsqGE/NH+W10AAAoSAEBgRPGudhRAN78RA0zlDTyI6p4Qoazgia0Kr3VcVaVcF4AAShIAQFemrT0ObxHCKX/GJApSwFzdCbhp9CArdPhpiJxN4K+9AAFKEgBAXmbblGkRE5RjXxJ/pFe442oMlgJQ4puABuZj3/2EDIHAAkoSAEBpCBe23Ry0PQ8RzAk3DdV4g0hO2vRbFGfo9CMXSMjpyAACqfP29U=";
////  // auto a2Fixed = "te6cckECHwEAAtgAAVsPin6lAAAAAAAAAACAHzT08s/AyLLbNObeCN4g8mgY2/7NGAHZDzRsGpmtGd7wAQlGA2rhGNnlLIJfis+p89WmG4DocIJxYVz8bO4ccthkpxChABECIgWBcAIDBChIAQF74DWmVhmM9SnkyaM4ahfHPja5mQMeAXa3H9XtbDVJkgAQIgEgBQYoSAEBXoRdqKQGacICk+FyI0A9f120dnKp77VVVQOQpbSktlgADyIBIAcIKEgBAV822Cc3DYLHgpJ4IiRYwA5laRssGBZinwfUY4ZNUnbkAA4iASAJCihIAQHGCfck4uvjQvvdsfcvbZora+TipEkgOKbsXdFMlQIAJQANIgEgCwwoSAEBRVAnHDn20YgprbXqEh/XEsz3B+i31dJQD/hRuwi5x5gACyIBIA0eIgEgDh0iASAPEChIAQEqXB5spVro6XvQbRAACcdVLSv4KFKiQGjhdoFtbTRQjAAJIgEgERIoSAEBscklhi4SzkZwnk68aNmbSpqi8AG4I4/33V7Z0Gi4Z5QABiIBIBMcIgEgFBUoSAEBxtwNQHBKtEIhgkmaLkculfbZzkxfdU8Po93uuNoGdYMAAyIBIBYbIgJzFxgoSAEB2YMA0DG1y5Wh/qc//wq3yv8qF1gM5+s4HyAnUF3nnTkAACIBWBkaAEW7p5Z+BkWW2ac28EbxB5NAxt/2aMAOyHmjYNTNaM73OohxqChIAQEaSg1G5+RVSCwvwsnC6olKYw1BE+WOcmLKhhPzR/ltdAAAKEgBAYETxrnYUQDe/EQNM5Q08iOqeEKGs4ImtCq91XFWlXBeAAEoSAEBXpq09Dm8Rwil/xiQKUsBc3Qm4afQgK3T4aYicTeCvvQABShIAQF5m25RpEROUY18Sf6RXuONqDJYCUOKbgAbmY9/9hAyBwAJKEgBAaQgXtt0ctD0PEcwJNw3VeINITtr0WxRn6PQjF0jI6cgAArp2a3a";
////  // auto code_cell = vm::std_boc_deserialize(td::base64_decode(td::Slice(
////  //   "te6cckEBBAEAVQAJRgPEA64KrWb+oOcX4JqBesAXEn8YnwcHhRvRRHp3DYsBVgABASIBwAIDAAMAYChIAQH0CV4ai6ETlmylTjgm83dyusoswyJRWv+o6ACNNzwAJAAAKD4d3w=="
////  // ))
////  //   .move_as_ok()).move_as_ok();
////
////  // SET_VERBOSITY_LEVEL(verbosity_INFO);
////  // td::set_default_failure_signal_handler().ensure();
////  //
////  // CHECK(vm::init_op_cp0());
////  //
////  // td::actor::ActorOwn<DbScanner> db_scanner_;
////  // td::actor::ActorOwn<ParseManager> parse_manager_;
////  // td::actor::ActorOwn<EventProcessor> event_processor_;
////  // td::actor::ActorOwn<InsertManagerPostgres> insert_manager_;
////  // td::actor::ActorOwn<MasterAddressLoader> index_scheduler_;
////  //
////  // // options
////  // uint32_t threads = 7;
////  // uint32_t stats_timeout = 10;
////  // std::string db_root;
////  // std::string working_dir;
////  // uint32_t last_known_seqno = 0;
////  //
////  // InsertManagerPostgres::Credential credential;
////  // credential.port = 9004;
////  // credential.dbname = "indexer";
////  // credential.user = "test";
////  // credential.password = "test";
////  //
////  // int max_active_tasks = 7;
////  // int max_insert_actors = 12;
////  //
////  // QueueState max_queue{200000, 200000, 1000000, 1000000};
////  // QueueState batch_size{2000, 2000, 10000, 10000};
////  //
////  // td::OptionParser p;
////  // p.set_description("Parse TON DB and insert data into Postgres");
////  // p.add_option('\0', "help", "prints_help", [&]() {
////  //   char b[10240];
////  //   td::StringBuilder sb(td::MutableSlice{b, 10000});
////  //   sb << p;
////  //   std::cout << sb.as_cslice().c_str();
////  //   std::exit(2);
////  // });
////  // p.add_option('D', "db", "Path to TON DB folder", [&](td::Slice fname) {
////  //   db_root = fname.str();
////  // });
////  // p.add_option('W', "working-dir", "Path to index working dir for secondary rocksdb logs", [&](td::Slice fname) {
////  //   working_dir = fname.str();
////  // });
////  // p.add_option('h', "host", "PostgreSQL host address", [&](td::Slice value) {
////  //   credential.host = value.str();
////  // });
////  // p.add_checked_option('p', "port", "PostgreSQL port", [&](td::Slice value) {
////  //   int port;
////  //   try {
////  //     port = std::stoi(value.str());
////  //     if (!(port >= 0 && port < 65536))
////  //       return td::Status::Error("Port must be a number between 0 and 65535");
////  //   } catch (...) {
////  //     return td::Status::Error(ton::ErrorCode::error, "bad value for --port: not a number");
////  //   }
////  //   credential.port = port;
////  //   return td::Status::OK();
////  // });
////  // p.add_option('u', "user", "PostgreSQL username", [&](td::Slice value) {
////  //   credential.user = value.str();
////  // });
////  // p.add_option('P', "password", "PostgreSQL password", [&](td::Slice value) {
////  //   credential.password = value.str();
////  // });
////  // p.add_option('d', "dbname", "PostgreSQL database name", [&](td::Slice value) {
////  //   credential.dbname = value.str();
////  // });
////  // auto S = p.run(argc, argv);
////  // if (S.is_error()) {
////  //   LOG(ERROR) << "failed to parse options: " << S.move_as_error();
////  //   std::_Exit(2);
////  // }
////  // if (working_dir.size() == 0) {
////  //   working_dir = PSTRING() << "/tmp/index_worker_" << getpid();
////  //   LOG(WARNING) << "Working dir not specified, using " << working_dir;
////  // }
////  //
////  // td::actor::Scheduler scheduler({threads});
////  // scheduler.run_in_context([&] {
////  //   insert_manager_ = td::actor::create_actor<InsertManagerPostgres>("insertmanager", credential);
////  // });
////  // scheduler.run_in_context([&] { parse_manager_ = td::actor::create_actor<ParseManager>("parsemanager"); });
////  // // todo uncomment
////  // // scheduler.run_in_context([&] {
////  // //   db_scanner_ = td::actor::create_actor<DbScanner>("scanner", "/Users/mac/CLionProjects/ton-index-worker/db", dbs_readonly, working_dir);
////  // // });
////  //
////  // auto masterStates = parse_items("/Users/mac/PycharmProjects/pythonProject/05_10_2024_res.txt");
////  // scheduler.run_in_context([&] {
////  //   index_scheduler_ = td::actor::create_actor<MasterAddressLoader>("loader", masterStates, //db_scanner_.get(),
////  //                                                                   insert_manager_.get(), parse_manager_.get());
////  // });
////  // scheduler.run_in_context([&] {
////  //   td::actor::send_closure(insert_manager_, &InsertManagerPostgres::init);
////  // });
////  //
////  // scheduler.run_in_context([&] {
////  //   td::actor::send_closure(insert_manager_, &InsertManagerPostgres::set_parallel_inserts_actors, max_insert_actors);
////  //   td::actor::send_closure(insert_manager_, &InsertManagerPostgres::set_insert_batch_size, batch_size);
////  //   td::actor::send_closure(insert_manager_, &InsertManagerPostgres::print_info);
////  // });
////  // scheduler.run_in_context([&] { td::actor::send_closure(index_scheduler_, &MasterAddressLoader::run); });
////  //
////  // while (scheduler.run(1)) {
////  //   // do something
////  // }
////
////  return 0;
////}
