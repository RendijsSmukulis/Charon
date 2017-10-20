using System;
using System.Collections.Generic;
using System.Text;

namespace Charon
{
    public interface IS3PathFilter
    {
        bool ShouldProcess(string bucket, string key);
    }
}
