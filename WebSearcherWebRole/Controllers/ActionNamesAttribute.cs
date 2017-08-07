using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Web.Mvc;

namespace WebSearcherWebRole
{

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
    public class ActionNamesAttribute : ActionNameSelectorAttribute
    {
        private readonly List<string> Names = new List<string>();

        public ActionNamesAttribute(params string[] names)
        {
            if (names != null)
            {
                foreach (string name in names)
                    if (!string.IsNullOrEmpty(name))
                        Names.Add(name);
                    else
                        throw new ArgumentException("ActionNames cannot be empty or null", "names");
            }
            else
                throw new ArgumentNullException("ActionNames cannot be empty or null", "names");
        }

        public override bool IsValidName(ControllerContext controllerContext, string actionName, MethodInfo methodInfo)
        {
            return Names.Any(x => String.Equals(actionName, x, StringComparison.OrdinalIgnoreCase));
        }

    }
}