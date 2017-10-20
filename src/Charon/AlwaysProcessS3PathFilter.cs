using System;

namespace Charon
{
    public class AlwaysProcessS3PathFilter: IS3PathFilter
    {
        public bool ShouldProcess(string bucket, string key)
        {
            return true;
        }
    }
}
