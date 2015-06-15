
//  Microsoft (R) .NET Framework IL Disassembler.  Version 4.0.30319.33440
//  Copyright (c) Microsoft Corporation.  All rights reserved.



// Metadata version: v4.0.30319
.assembly extern mscorlib
{
  .publickeytoken = (B7 7A 5C 56 19 34 E0 89 )                         // .z\V.4..
  .ver 4:0:0:0
}
.assembly GenericPointerHelpers
{
  .custom instance void [mscorlib]System.Runtime.CompilerServices.CompilationRelaxationsAttribute::.ctor(int32) = ( 01 00 08 00 00 00 00 00 ) 
  .custom instance void [mscorlib]System.Runtime.CompilerServices.RuntimeCompatibilityAttribute::.ctor() = ( 01 00 01 00 54 02 16 57 72 61 70 4E 6F 6E 45 78   // ....T..WrapNonEx
                                                                                                             63 65 70 74 69 6F 6E 54 68 72 6F 77 73 01 )       // ceptionThrows.

  // --- The following custom attribute is added automatically, do not uncomment -------
  //  .custom instance void [mscorlib]System.Diagnostics.DebuggableAttribute::.ctor(valuetype [mscorlib]System.Diagnostics.DebuggableAttribute/DebuggingModes) = ( 01 00 02 00 00 00 00 00 ) 

  .custom instance void [mscorlib]System.Reflection.AssemblyTitleAttribute::.ctor(string) = ( 01 00 15 47 65 6E 65 72 69 63 50 6F 69 6E 74 65   // ...GenericPointe
                                                                                              72 48 65 6C 70 65 72 73 00 00 )                   // rHelpers..
  .custom instance void [mscorlib]System.Reflection.AssemblyDescriptionAttribute::.ctor(string) = ( 01 00 00 00 00 ) 
  .custom instance void [mscorlib]System.Reflection.AssemblyConfigurationAttribute::.ctor(string) = ( 01 00 00 00 00 ) 
  .custom instance void [mscorlib]System.Reflection.AssemblyCompanyAttribute::.ctor(string) = ( 01 00 00 00 00 ) 
  .custom instance void [mscorlib]System.Reflection.AssemblyProductAttribute::.ctor(string) = ( 01 00 15 47 65 6E 65 72 69 63 50 6F 69 6E 74 65   // ...GenericPointe
                                                                                                72 48 65 6C 70 65 72 73 00 00 )                   // rHelpers..
  .custom instance void [mscorlib]System.Reflection.AssemblyCopyrightAttribute::.ctor(string) = ( 01 00 12 43 6F 70 79 72 69 67 68 74 20 C2 A9 20   // ...Copyright .. 
                                                                                                  20 32 30 31 35 00 00 )                            //  2015..
  .custom instance void [mscorlib]System.Reflection.AssemblyTrademarkAttribute::.ctor(string) = ( 01 00 00 00 00 ) 
  .custom instance void [mscorlib]System.Runtime.InteropServices.ComVisibleAttribute::.ctor(bool) = ( 01 00 00 00 00 ) 
  .custom instance void [mscorlib]System.Runtime.InteropServices.GuidAttribute::.ctor(string) = ( 01 00 24 66 66 36 36 63 61 63 62 2D 65 36 36 65   // ..$ff66cacb-e66e
                                                                                                  2D 34 64 62 36 2D 38 36 66 36 2D 34 66 36 39 64   // -4db6-86f6-4f69d
                                                                                                  35 65 34 30 62 32 62 00 00 )                      // 5e40b2b..
  .custom instance void [mscorlib]System.Reflection.AssemblyFileVersionAttribute::.ctor(string) = ( 01 00 07 31 2E 30 2E 30 2E 30 00 00 )             // ...1.0.0.0..
  .custom instance void [mscorlib]System.Runtime.Versioning.TargetFrameworkAttribute::.ctor(string) = ( 01 00 1A 2E 4E 45 54 46 72 61 6D 65 77 6F 72 6B   // ....NETFramework
                                                                                                        2C 56 65 72 73 69 6F 6E 3D 76 34 2E 30 01 00 54   // ,Version=v4.0..T
                                                                                                        0E 14 46 72 61 6D 65 77 6F 72 6B 44 69 73 70 6C   // ..FrameworkDispl
                                                                                                        61 79 4E 61 6D 65 10 2E 4E 45 54 20 46 72 61 6D   // ayName..NET Fram
                                                                                                        65 77 6F 72 6B 20 34 )                            // ework 4
  .permissionset reqmin
             = {[mscorlib]System.Security.Permissions.SecurityPermissionAttribute = {property bool 'SkipVerification' = bool(true)}}
  .hash algorithm 0x00008004
  .ver 1:0:0:0
}
.module GenericPointerHelpers.dll
// MVID: {EC5C576B-46D7-4E96-BA9F-6FD95CA1EDD9}
.custom instance void [mscorlib]System.Security.UnverifiableCodeAttribute::.ctor() = ( 01 00 00 00 ) 
.imagebase 0x10000000
.file alignment 0x00000200
.stackreserve 0x00100000
.subsystem 0x0003       // WINDOWS_CUI
.corflags 0x00000001    //  ILONLY
// Image base: 0x02E80000


// =============== CLASS MEMBERS DECLARATION ===================

.class public abstract auto ansi sealed beforefieldinit GenericPointerHelpers.GenericPointerHelper
       extends [mscorlib]System.Object
{
  .method public hidebysig static !!T  Read<T>(void* ptr) cil managed
  {
    // Code size       6 (0x6)
    .maxstack  8
    IL_0000:  nop         
	IL_0001:  ldarg.0     
	IL_0002:  ldobj !!T
	IL_0003:  ret 
  } // end of method GenericPointerHelper::Read

  .method public hidebysig static void  Write<T>(void* ptr,
                                                 !!T 'value') cil managed
  {
    // Code size       6 (0x6)
    .maxstack  8
    IL_0000:  ldarg.0
    IL_0001:  ldarg.1
	IL_0002:  stobj !!T
    IL_0003:  ret
  } // end of method GenericPointerHelper::Write

  .method public hidebysig static int32  SizeOf<T>() cil managed
  {
    // Code size       6 (0x6)
    .maxstack  8
    IL_0000: sizeof !!T
    IL_0003: ret
  } // end of method GenericPointerHelper::Write

  
  .method public hidebysig static void CopyBytes(void* from, void* 'to', unsigned int32 'count') cil managed
  {
    // Code size       6 (0x6)
    .maxstack  8
    ldarg.1
    // unaligned. 1
    ldarg.0
    // unaligned. 1
    ldarg.2
	unaligned. 1
    cpblk
    ret
  } // end of method GenericPointerHelper::Write

  .method public hidebysig static void CopyBytesAlligned(void* from, void* 'to', unsigned int32 'count') cil managed
  {
    // Code size       6 (0x6)
    .maxstack  8
    ldarg.1
    // unaligned. 1
    ldarg.0
    // unaligned. 1
    ldarg.2
    cpblk
    ret
  } // end of method GenericPointerHelper::Write


} // end of class GenericPointerHelpers.GenericPointerHelper


// =============================================================

// *********** DISASSEMBLY COMPLETE ***********************
// WARNING: Created Win32 resource file C:\Users\exyi\Source\Repos\RaptorDB-Document\GenericPointerHelpers\assembly2.res
